//! Plain helpers of the Linux-only `netns` module: nft script builders.
//! They run no commands, so they live outside `netns` and their tests run
//! on every host.

use std::net::Ipv4Addr;

pub(crate) fn egress_table_name(task_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(task_id.as_bytes());
    format!(
        "minga_egr_{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7]
    )
}

/// Removes the task's `inet` and `bridge` egress tables. Adding each table
/// first makes a missing one a no-op inside the same nft transaction, so a
/// failure of this script is always real.
pub(crate) fn egress_delete_script(table: &str) -> String {
    format!(
        "add table inet {table}\n\
         delete table inet {table}\n\
         add table bridge {table}\n\
         delete table bridge {table}\n"
    )
}

/// Per-task nft rules for a required-egress task, replacing any previous
/// ones in the same transaction.
///
/// With `br_netfilter` (always loaded by the task source guard) the inet
/// hooks see the bridge, not the task's veth, as the input interface, for
/// routed and same-bridge IPv4 alike. The source guard pins each veth to
/// the task's own address, so bridge plus source address identifies the
/// task. Only DNS and the proxy on the gateway are open; replies to
/// connections made *to* the task stay allowed.
///
/// Those rules and the source guard match IPv4 only. IPv6 is disabled in
/// the task netns, but a container keeps `CAP_NET_RAW` and can still put
/// IPv6 (or any other non-IPv4) frames on its veth. A per-bridge inet drop
/// would also cut every other task on the bridge and miss same-bridge
/// frames when `bridge-nf-call-ip6tables` is off, so a bridge-family
/// prerouting chain drops everything but IPv4 and ARP on the task's own
/// bridge ports, before the bridge delivers, forwards or routes it.
pub(crate) fn egress_policy_script(
    table: &str,
    ips: &[Ipv4Addr],
    bridges: &[String],
    gateways: &[Ipv4Addr],
    veths: &[String],
    proxy_port: u16,
) -> String {
    let mut script = egress_delete_script(table);
    script.push_str(&format!(
        "add table inet {table}\n\
         add chain inet {table} input {{ type filter hook input priority filter; policy accept; }}\n\
         add chain inet {table} forward {{ type filter hook forward priority filter; policy accept; }}\n"
    ));
    for ((ip, bridge), gateway) in ips.iter().zip(bridges).zip(gateways) {
        script.push_str(&format!(
            "add rule inet {table} input iifname \"{bridge}\" ip saddr {ip} ip daddr {gateway} udp dport 53 accept\n\
             add rule inet {table} input iifname \"{bridge}\" ip saddr {ip} ip daddr {gateway} tcp dport 53 accept\n\
             add rule inet {table} input iifname \"{bridge}\" ip saddr {ip} ip daddr {gateway} tcp dport {proxy_port} accept\n\
             add rule inet {table} input iifname \"{bridge}\" ip saddr {ip} ct state established,related accept\n\
             add rule inet {table} input iifname \"{bridge}\" ip saddr {ip} drop\n\
             add rule inet {table} forward iifname \"{bridge}\" ip saddr {ip} ct state established,related accept\n\
             add rule inet {table} forward iifname \"{bridge}\" ip saddr {ip} drop\n"
        ));
    }
    script.push_str(&format!(
        "add table bridge {table}\n\
         add chain bridge {table} prerouting {{ type filter hook prerouting priority filter; policy accept; }}\n"
    ));
    for veth in veths {
        script.push_str(&format!(
            "add rule bridge {table} prerouting iifname \"{veth}\" ether type != {{ ip, arp }} drop\n"
        ));
    }
    script
}

/// Host table that limits the egress proxy port to loopback and task
/// bridges, so the listener is not reachable from the LAN or the mesh.
pub(crate) const EGRESS_LISTENER_TABLE: &str = "minga_egress_listener";

pub(crate) fn egress_listener_script(port: u16) -> String {
    let t = EGRESS_LISTENER_TABLE;
    format!(
        "add table inet {t}\n\
         add chain inet {t} input {{ type filter hook input priority filter; policy accept; }}\n\
         add rule inet {t} input tcp dport {port} iifname \"lo\" accept\n\
         add rule inet {t} input tcp dport {port} iifname \"{wg}\" drop\n\
         add rule inet {t} input tcp dport {port} iifname \"minga-*\" accept\n\
         add rule inet {t} input tcp dport {port} drop\n",
        wg = minga_net::wireguard::WG_INTERFACE_NAME,
    )
}

pub(crate) fn egress_delete_args(table: &str) -> [&str; 4] {
    ["delete", "table", "inet", table]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_egress_rules_allow_proxy_only_and_cleanup_is_task_scoped() {
        let ip = Ipv4Addr::new(10, 44, 1, 7);
        let gateway = Ipv4Addr::new(10, 44, 1, 1);
        let bridge = "minga-abcd".to_string();
        let veth = "mhabcd1234".to_string();
        let table = egress_table_name("stack.web.1");
        let script = egress_policy_script(&table, &[ip], &[bridge], &[gateway], &[veth], 8080);
        assert!(script.contains("daddr 10.44.1.1 tcp dport 8080 accept"));
        assert!(script.contains("udp dport 53 accept"));
        // br_netfilter shows the bridge, not the veth, to the inet hooks.
        assert!(script.contains("forward iifname \"minga-abcd\" ip saddr 10.44.1.7 drop"));
        assert!(script.contains("input iifname \"minga-abcd\" ip saddr 10.44.1.7 drop"));
        assert!(script
            .lines()
            .filter(|line| line.starts_with("add rule inet"))
            .all(|line| !line.contains("mhabcd1234")));
        let listener = egress_listener_script(9090);
        assert!(listener.contains("tcp dport 9090 iifname \"minga-wg0\" drop"));
        assert!(listener.ends_with("tcp dport 9090 drop\n"));
        assert_ne!(table, egress_table_name("stack.web.2"));
        assert_eq!(
            egress_delete_args(&table),
            ["delete", "table", "inet", table.as_str()]
        );
    }

    #[test]
    fn required_egress_drops_non_ipv4_frames_on_the_task_port_only() {
        let table = egress_table_name("stack.web.1");
        let script = egress_policy_script(
            &table,
            &[Ipv4Addr::new(10, 44, 1, 7), Ipv4Addr::new(10, 45, 1, 7)],
            &["minga-abcd".into(), "minga-ef01".into()],
            &[Ipv4Addr::new(10, 44, 1, 1), Ipv4Addr::new(10, 45, 1, 1)],
            &["mhaaaa".into(), "mhbbbb".into()],
            8080,
        );
        // The task's own bridge ports, never a whole bridge: other tasks keep
        // their IPv6, and IPv4 and ARP (replies included) still pass.
        for veth in ["mhaaaa", "mhbbbb"] {
            assert!(script.contains(&format!(
                "add rule bridge {table} prerouting iifname \"{veth}\" ether type != {{ ip, arp }} drop\n"
            )));
        }
        assert!(script.contains(&format!(
            "add chain bridge {table} prerouting {{ type filter hook prerouting priority filter; policy accept; }}"
        )));
        assert!(!script.contains("iifname \"minga-abcd\" ether"));
        assert!(!script.contains("nfproto ipv6"));
        // Old rules of both families go in the same transaction.
        assert!(script.starts_with(&egress_delete_script(&table)));
        let delete = egress_delete_script(&table);
        for family in ["inet", "bridge"] {
            assert!(delete.contains(&format!(
                "add table {family} {table}\ndelete table {family} {table}\n"
            )));
        }
    }
}
