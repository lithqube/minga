//! Output rendering: table, JSON and YAML.
//!
//! Results go to stdout; progress, prompts and errors go to stderr. JSON and
//! YAML output is an array of the same rows the table shows, with snake_case
//! keys.

use std::io::Write;

use serde::Serialize;

use crate::error::CtlError;

/// How a command prints its rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum Format {
    /// Padded columns.
    #[default]
    Table,
    /// A JSON array.
    Json,
    /// A YAML sequence.
    Yaml,
}

/// Rows to print, with the columns for the table and the values for JSON.
#[derive(Debug, Clone)]
pub struct Rows {
    /// Column headings, in order.
    pub headers: Vec<String>,
    /// Cell text, one vector per row.
    pub cells: Vec<Vec<String>>,
    /// The same rows as JSON values.
    pub values: Vec<serde_json::Value>,
    /// Column that holds the row's identifier; quiet mode prints only it.
    pub id_column: usize,
}

impl Rows {
    /// An empty result whose identifier is the first column.
    pub fn new(headers: &[&str]) -> Self {
        Rows {
            headers: headers.iter().map(|h| h.to_string()).collect(),
            cells: Vec::new(),
            values: Vec::new(),
            id_column: 0,
        }
    }

    /// Sets the column that holds the row's identifier.
    pub fn with_id_column(mut self, column: usize) -> Self {
        self.id_column = column;
        self
    }

    /// Adds a row.
    pub fn push(&mut self, cells: Vec<String>, value: serde_json::Value) {
        self.cells.push(cells);
        self.values.push(value);
    }

    /// Whether there is anything to print.
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }
}

/// Prints rows in the requested format.
///
/// Quiet mode prints only the identifier column of each row, one per line.
/// A failed write to stdout (a closed pipe, a full disk) is an error, so a
/// caller never reports success for incomplete output.
pub fn print(format: Format, quiet: bool, rows: &Rows) -> Result<(), CtlError> {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let written = match format {
        Format::Table if quiet => rows
            .cells
            .iter()
            .filter_map(|cells| cells.get(rows.id_column))
            .try_for_each(|id| writeln!(out, "{id}")),
        Format::Table => write_table(&mut out, rows),
        Format::Json => {
            let text = serde_json::to_string_pretty(&rows.values)?;
            writeln!(out, "{text}")
        }
        Format::Yaml => {
            let yaml =
                serde_yaml::to_string(&rows.values).map_err(|e| CtlError::Output(e.to_string()))?;
            write!(out, "{yaml}")
        }
    };
    written
        .and_then(|()| out.flush())
        .map_err(|e| CtlError::Output(format!("write to stdout: {e}")))
}

/// Prints a one-line confirmation to stdout unless `quiet` is set.
pub fn say(quiet: bool, message: &str) -> Result<(), CtlError> {
    if quiet {
        return Ok(());
    }
    let mut out = std::io::stdout().lock();
    writeln!(out, "{message}")
        .and_then(|()| out.flush())
        .map_err(|e| CtlError::Output(format!("write to stdout: {e}")))
}

/// Writes the padded table.
fn write_table<W: Write>(out: &mut W, rows: &Rows) -> std::io::Result<()> {
    if rows.cells.is_empty() {
        return Ok(());
    }
    let widths: Vec<usize> = rows
        .headers
        .iter()
        .enumerate()
        .map(|(i, header)| {
            let longest = rows
                .cells
                .iter()
                .filter_map(|cells| cells.get(i))
                .map(|cell| cell.chars().count())
                .max()
                .unwrap_or(0);
            header.chars().count().max(longest)
        })
        .collect();

    let last = rows.headers.len().saturating_sub(1);
    let mut line = String::new();
    for (i, header) in rows.headers.iter().enumerate() {
        if i > 0 {
            line.push_str("  ");
        }
        line.push_str(&pad(header, widths[i], i == last));
    }
    writeln!(out, "{}", line.trim_end())?;

    for cells in &rows.cells {
        let mut line = String::new();
        for (i, width) in widths.iter().enumerate() {
            if i > 0 {
                line.push_str("  ");
            }
            let cell = cells.get(i).map(String::as_str).unwrap_or("");
            line.push_str(&pad(cell, *width, i == last));
        }
        writeln!(out, "{}", line.trim_end())?;
    }
    Ok(())
}

/// Pads a cell to a column width; the last column is not padded.
fn pad(cell: &str, width: usize, last: bool) -> String {
    if last {
        return cell.to_string();
    }
    let len = cell.chars().count();
    let mut out = String::from(cell);
    if len < width {
        out.push_str(&" ".repeat(width - len));
    }
    out
}

/// Builds the JSON value for a context row.
pub fn context_value(
    current: bool,
    name: &str,
    transport: &str,
    ssh: Option<&str>,
    endpoint: &str,
    token: &str,
) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    map.insert("current".into(), serde_json::Value::Bool(current));
    map.insert("name".into(), serde_json::Value::String(name.to_string()));
    map.insert(
        "transport".into(),
        serde_json::Value::String(transport.to_string()),
    );
    if let Some(ssh) = ssh {
        map.insert("ssh".into(), serde_json::Value::String(ssh.to_string()));
    }
    map.insert(
        "endpoint".into(),
        serde_json::Value::String(endpoint.to_string()),
    );
    map.insert("token".into(), serde_json::Value::String(token.to_string()));
    serde_json::Value::Object(map)
}

/// Serializes a value for a JSON output line.
pub fn to_json<T: Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_rows() -> Rows {
        let mut rows = Rows::new(&["NAME", "COUNT"]);
        rows.push(
            vec!["myapp".to_string(), "2".to_string()],
            serde_json::json!({"name": "myapp", "count": 2}),
        );
        rows.push(
            vec!["shop".to_string(), "5".to_string()],
            serde_json::json!({"name": "shop", "count": 5}),
        );
        rows
    }

    #[test]
    fn json_output_is_an_array_of_rows() {
        let rows = sample_rows();
        let text = serde_json::to_string(&rows.values).unwrap();
        assert!(text.starts_with('['), "{text}");
        assert!(text.contains("\"name\":\"myapp\""), "{text}");
    }

    #[test]
    fn the_table_pads_columns() {
        let rows = sample_rows();
        let mut out = Vec::new();
        write_table(&mut out, &rows).unwrap();
        let text = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], "NAME   COUNT");
        assert_eq!(lines[1], "myapp  2");
        assert_eq!(lines[2], "shop   5");
    }

    #[test]
    fn a_failed_write_is_reported() {
        struct Closed;
        impl Write for Closed {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let err = write_table(&mut Closed, &sample_rows()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
    }

    #[test]
    fn an_empty_table_prints_nothing() {
        let rows = Rows::new(&["NAME"]);
        let mut out = Vec::new();
        write_table(&mut out, &rows).unwrap();
        assert!(out.is_empty());
    }
}
