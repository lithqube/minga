use rustls::pki_types::CertificateDer;
use x509_parser::prelude::parse_x509_certificate;

/// A role encoded in a client certificate's subject OU (ADR 0005).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ApiRole {
    Viewer,
    Agent,
    Deployer,
    Admin,
}

impl ApiRole {
    /// Every role name a certificate's OU may carry.
    pub const NAMES: [&'static str; 4] = ["viewer", "agent", "deployer", "admin"];

    /// The role named by a certificate OU, if it is one of [`Self::NAMES`].
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "viewer" => Some(Self::Viewer),
            "agent" => Some(Self::Agent),
            "deployer" => Some(Self::Deployer),
            "admin" => Some(Self::Admin),
            _ => None,
        }
    }

    /// The OU value for this role.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::Agent => "agent",
            Self::Deployer => "deployer",
            Self::Admin => "admin",
        }
    }

    /// Whether this role may do what `minimum` requires. Roles are ordered:
    /// agent includes viewer; deployer includes both; admin includes all.
    #[must_use]
    pub fn permits(self, minimum: Self) -> bool {
        self >= minimum
    }
}

/// Identity fields parsed from a CA-verified client certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIdentity {
    /// The certificate subject CN.
    pub name: String,
    /// `None` when the OU is not a known role.
    pub role: Option<ApiRole>,
    /// `None` means the certificate has no O; `Some("*")` is a cluster operator.
    pub tenant: Option<String>,
}

impl ClientIdentity {
    /// Parses the subject CN, exactly one OU, and zero or one O.
    pub fn from_certificate(cert: &CertificateDer<'_>) -> Option<Self> {
        let (_, certificate) = parse_x509_certificate(cert.as_ref()).ok()?;
        let subject = certificate.subject();
        let common_names = subject
            .iter_common_name()
            .map(|attribute| attribute.as_str().ok())
            .collect::<Option<Vec<_>>>()?;
        let roles = subject
            .iter_organizational_unit()
            .map(|attribute| attribute.as_str().ok())
            .collect::<Option<Vec<_>>>()?;
        let tenants = subject
            .iter_organization()
            .map(|attribute| attribute.as_str().ok())
            .collect::<Option<Vec<_>>>()?;
        if common_names.len() != 1 || roles.len() != 1 || tenants.len() > 1 {
            return None;
        }
        let name = common_names.first()?.trim();
        if name.is_empty() {
            return None;
        }
        Some(Self {
            name: name.to_owned(),
            role: ApiRole::parse(roles.first()?),
            tenant: tenants.first().map(|value| value.trim().to_owned()),
        })
    }
}

/// Resolved request identity. Token and unauthenticated compatibility callers
/// are confined to the default tenant and lose workload access once it is no
/// longer the cluster's only tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    /// The certificate CN, or `token` / `legacy-client`.
    pub name: String,
    /// What the caller may do inside its scope.
    pub role: ApiRole,
    /// The one tenant the caller may touch; `None` for a cluster operator.
    pub tenant_id: Option<String>,
    /// May use cluster-wide endpoints (`/minga/*`, nodes, info).
    pub cluster_operator: bool,
}

impl Caller {
    /// The caller's tenant, if it is confined to one.
    #[must_use]
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::{ApiRole, ClientIdentity};
    use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};

    #[test]
    fn client_identity_reads_role_and_tenant_from_subject() {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        let mut subject = DistinguishedName::new();
        subject.push(DnType::CommonName, "deploy-bot");
        subject.push(DnType::OrganizationalUnitName, "deployer");
        subject.push(DnType::OrganizationName, "tenant-a");
        params.distinguished_name = subject;
        let certificate = params.self_signed(&key).unwrap();

        let identity = ClientIdentity::from_certificate(certificate.der()).unwrap();
        assert_eq!(identity.name, "deploy-bot");
        assert_eq!(identity.role, Some(ApiRole::Deployer));
        assert_eq!(identity.tenant.as_deref(), Some("tenant-a"));
    }
}
