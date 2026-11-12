//! Serialization for everything `minga-state` persists or sends to a peer.
//!
//! JSON, not bincode. Bincode is not self-describing, so adding a field to a
//! stored type made old data undecodable (the 1.0.1 to 1.0.2 upgrade needed a
//! cleared `--data-dir`). With JSON a field marked `#[serde(default)]` can be
//! added without touching existing data.
//!
//! # Rules for changing a persisted type
//!
//! - New field: give it `#[serde(default)]`. Nothing else is needed.
//! - Renaming, removing or re-typing a field, or changing what a value means:
//!   bump [`FORMAT_VERSION`] and add a migration step in [`check_format`].
//!
//! # Format marker
//!
//! Each redb file stores [`FORMAT_VERSION`] under [`FORMAT_KEY`] in its
//! metadata table. A store with data and no marker was written by minga 1.0.2
//! or older (bincode) and cannot be read.
//!
//! # Wire frame
//!
//! A Raft message is `[u32 BE length][u8 FORMAT_VERSION][JSON]`, the length
//! covering the version byte and the JSON.

use serde::de::DeserializeOwned;
use serde::Serialize;

/// Version of the persisted and wire format. Bump it when old data can no
/// longer be read as is (see the rules above).
pub const FORMAT_VERSION: u8 = 2;

/// Metadata key that holds [`FORMAT_VERSION`] in each redb file.
pub const FORMAT_KEY: &str = "format_version";

/// Failure to encode, decode or accept stored data.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("{0}")]
    Json(#[from] serde_json::Error),
    #[error("empty message")]
    EmptyFrame,
    #[error("peer sends wire format {got}, this node speaks {want}; upgrade every node together")]
    WireVersion { got: u8, want: u8 },
    #[error(
        "this data directory was written by minga 1.0.2 or older and cannot be read: \
         remove raft_log.redb and state.redb from --data-dir (not the whole directory) and redeploy"
    )]
    OldFormat,
    #[error(
        "this data directory was written by a newer minga (format {found}, this build \
         reads {want}): use the newer binary"
    )]
    NewerFormat { found: u8, want: u8 },
    #[error(
        "no migration from data format {from} to {want}: remove raft_log.redb and state.redb \
         from --data-dir (not the whole directory) and redeploy"
    )]
    NoMigration { from: u8, want: u8 },
    #[error("unreadable format_version marker: {0:?}")]
    BadMarker(Vec<u8>),
}

/// Encodes a value for storage.
pub fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, CodecError> {
    Ok(serde_json::to_vec(value)?)
}

/// Decodes a stored value.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Encodes a value for the wire: [`FORMAT_VERSION`] byte, then JSON.
pub fn encode_wire<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, CodecError> {
    let mut frame = vec![FORMAT_VERSION];
    serde_json::to_writer(&mut frame, value)?;
    Ok(frame)
}

/// Decodes a wire frame, refusing a peer with another format version.
pub fn decode_wire<T: DeserializeOwned>(frame: &[u8]) -> Result<T, CodecError> {
    match frame.split_first() {
        Some((&FORMAT_VERSION, body)) => decode(body),
        Some((&got, _)) => Err(CodecError::WireVersion {
            got,
            want: FORMAT_VERSION,
        }),
        None => Err(CodecError::EmptyFrame),
    }
}

/// Checks a store's format marker when it is opened.
///
/// `stored` is the value under [`FORMAT_KEY`] and `is_empty` says whether the
/// file holds no data yet. Returns `true` when the caller must write the
/// current marker (a new store).
pub fn check_format(stored: Option<&[u8]>, is_empty: bool) -> Result<bool, CodecError> {
    match stored {
        None if is_empty => Ok(true),
        None => Err(CodecError::OldFormat),
        Some([FORMAT_VERSION]) => Ok(false),
        Some(&[found]) if found > FORMAT_VERSION => Err(CodecError::NewerFormat {
            found,
            want: FORMAT_VERSION,
        }),
        // shortcut: no migration steps exist yet, so an older marker is refused;
        // add the steps here when FORMAT_VERSION is bumped.
        Some(&[from]) => Err(CodecError::NoMigration {
            from,
            want: FORMAT_VERSION,
        }),
        Some(other) => Err(CodecError::BadMarker(other.to_vec())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, serde::Deserialize)]
    struct Stored {
        name: String,
        #[serde(default)]
        added_later: Vec<u32>,
    }

    #[test]
    fn a_defaulted_field_can_be_added_after_data_was_written() {
        let old = br#"{"name":"web"}"#;
        let got: Stored = decode(old).unwrap();
        assert_eq!(
            got,
            Stored {
                name: "web".into(),
                added_later: vec![]
            }
        );
    }

    #[test]
    fn a_field_a_newer_build_wrote_is_ignored() {
        let newer = br#"{"name":"web","added_later":[1],"from_the_future":true}"#;
        let got: Stored = decode(newer).unwrap();
        assert_eq!(got.added_later, vec![1]);
    }

    #[test]
    fn wire_frames_carry_the_version_and_refuse_another() {
        let value = Stored {
            name: "web".into(),
            added_later: vec![7],
        };
        let frame = encode_wire(&value).unwrap();
        assert_eq!(frame[0], FORMAT_VERSION);
        assert_eq!(decode_wire::<Stored>(&frame).unwrap(), value);

        let mut other = frame.clone();
        other[0] = FORMAT_VERSION + 1;
        let err = decode_wire::<Stored>(&other).unwrap_err();
        assert!(matches!(err, CodecError::WireVersion { .. }), "{err}");
        assert!(matches!(
            decode_wire::<Stored>(&[]).unwrap_err(),
            CodecError::EmptyFrame
        ));
    }

    #[test]
    fn the_marker_is_checked_when_a_store_is_opened() {
        // A new store gets the marker; a current one is left alone.
        assert!(check_format(None, true).unwrap());
        assert!(!check_format(Some(&[FORMAT_VERSION]), false).unwrap());

        // Data without a marker is what 1.0.2 wrote.
        let err = check_format(None, false).unwrap_err();
        assert!(err.to_string().contains("1.0.2 or older"), "{err}");
        assert!(
            err.to_string()
                .contains("remove raft_log.redb and state.redb"),
            "{err}"
        );

        let newer = check_format(Some(&[FORMAT_VERSION + 1]), false).unwrap_err();
        assert!(newer.to_string().contains("newer minga"), "{newer}");

        assert!(matches!(
            check_format(Some(&[FORMAT_VERSION - 1]), false).unwrap_err(),
            CodecError::NoMigration { .. }
        ));
        assert!(matches!(
            check_format(Some(&[1, 2]), false).unwrap_err(),
            CodecError::BadMarker(_)
        ));
    }
}
