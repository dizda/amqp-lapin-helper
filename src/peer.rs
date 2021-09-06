use models::check::Check;
use models::semver::Semver;
use models::{NodeServer, Platform};
use serde::Serialize;
use std::net::IpAddr;
use uuid::Uuid;

pub const PEER_EXCHANGE_NAME: &'static str = "peer";
pub const PEER_ROUTING_KEY_ADD: &'static str = "peer_connection";
pub const PEER_ROUTING_KEY_ROUTINE_CHECK: &'static str = "peer_routine_check";
pub const PEER_ROUTING_KEY_READY: &'static str = "peer_ready";
pub const PEER_ROUTING_KEY_UPDATE: &'static str = "peer_update";
pub const PEER_ROUTING_KEY_DELETE: &'static str = "peer_disconnection";

// #[serde(tag = "action")] // doesn't work with `bincode`
// TODO: Since we migrated to bincode, maybe the best way is to use routing_key to fill the enum rather than sending the enum?, we would save 1 byte
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerEvent<'a> {
    /// The node will send this event when a peer connects to it
    Add {
        node: &'a NodeServer, //TODO: rename to NodeMessage?
        peer: PeerMessage<'a>,
    }, //todo: add node name/id maybe?
    /// The node will send this event when a peer has spent enough time
    /// and needs to be checked again.
    RoutineCheck {
        node: &'a NodeServer,
        peer: PeerMessage<'a>,
    }, //todo: add node name/id maybe?
    /// Once the `consumer_peer_check` has successfully checked the Peer and suggest it is
    /// ready for use, it'll send this `Ready` event to the load balancers
    Ready { peer: &'a models::PeerLb },
    /// The `consumer_peer_check` has successfully checked again the peer
    /// and send an `Update` event.
    ///
    /// This event is important, because sometime a Peer can switch network without disconnection
    /// (from wifi to 4G), the WiFi's IP is in one country, and the SimCard has been registered
    /// in a different country, so the country (and IP) will change and needs to make sure
    /// the peer has been removed from the previous country, otherwise, on the `DELETE` event
    /// the Peer won't be deleted from the previous country.
    Update { peer: &'a models::PeerLb },
    /// The `consumer_peer_remove` will dispatch a Delete event to remove a peer.
    Delete { node_id: u32, stream_id: u32 },
}

#[derive(Debug, Serialize)]
pub struct PeerMessage<'a> {
    pub id: u32,
    pub ip: &'a IpAddr,
    pub app_id: u32,
    pub peer_uuid: Option<&'a Uuid>,
    pub device_uuid: &'a Uuid,
    pub platform: &'a Platform,
    pub peer_version: &'a Semver,
}

impl crate::BrokerPublish for PeerEvent<'_> {
    fn exchange_name(&self) -> &'static str {
        PEER_EXCHANGE_NAME
    }

    fn routing_key(&self) -> &'static str {
        match self {
            PeerEvent::Add { .. } => PEER_ROUTING_KEY_ADD,
            PeerEvent::RoutineCheck { .. } => PEER_ROUTING_KEY_ROUTINE_CHECK,
            PeerEvent::Ready { .. } => PEER_ROUTING_KEY_READY,
            PeerEvent::Update { .. } => PEER_ROUTING_KEY_UPDATE,
            PeerEvent::Delete { .. } => PEER_ROUTING_KEY_DELETE,
        }
    }
}

/// Can not use the normal PeerEvent as it uses borrowed data,
/// and because we're streaming, this just doesn't work
pub mod deserialize {
    use models::check::Check;
    use models::semver::Semver;
    use models::{NodeServer, Platform};
    use serde::Deserialize;
    use std::net::IpAddr;
    use uuid::Uuid;

    // #[serde(tag = "action")] // doesn't work with `bincode`
    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum PeerEvent {
        Add { node: NodeServer, peer: PeerMessage },
        RoutineCheck { node: NodeServer, peer: PeerMessage },
        Ready { peer: models::PeerLb },
        Update { peer: models::PeerLb },
        Delete { node_id: u32, stream_id: u32 },
    }

    #[derive(Debug, Deserialize)]
    pub struct PeerMessage {
        pub id: u32,
        pub ip: IpAddr,
        pub app_id: u32,
        pub peer_uuid: Option<Uuid>,
        pub device_uuid: Uuid,
        pub platform: Platform,
        pub peer_version: Semver,
    }

    impl<T: Check> From<PeerEvent> for models::Peer<T> {
        fn from(event: PeerEvent) -> Self {
            match event {
                PeerEvent::Add { node, peer } => Self::new(
                    node.id.unwrap(),
                    &node.ipv4,
                    peer.app_id,
                    peer.id,
                    &peer.ip,
                    peer.peer_uuid,
                    peer.device_uuid,
                    peer.platform,
                    peer.peer_version,
                    chrono::Utc::now().into(),
                ),
                PeerEvent::RoutineCheck { node, peer } => Self::new(
                    node.id.unwrap(),
                    &node.ipv4,
                    peer.app_id,
                    peer.id,
                    &peer.ip,
                    peer.peer_uuid,
                    peer.device_uuid,
                    peer.platform,
                    peer.peer_version,
                    chrono::Utc::now().into(),
                ),
                PeerEvent::Ready { .. } => {
                    unimplemented!("Need to implement From<PeerEvent<T>>::Ready")
                }
                PeerEvent::Update { .. } => {
                    unimplemented!("Need to implement From<PeerEvent<T>>::Update")
                }
                PeerEvent::Delete { .. } => {
                    unimplemented!("Need to implement From<PeerEvent<T>>::Delete")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::peer::deserialize::PeerEvent as PeerEventDes;
    use crate::peer::PeerEvent;
    use models::check::SkipCheck;

    #[test]
    fn supports_bincode() {
        let event: PeerEvent = PeerEvent::Delete {
            node_id: 32,
            stream_id: 234,
        };
        let test = bincode::serialize(&event).unwrap();
        assert_eq!(vec![4, 0, 0, 0, 32, 0, 0, 0, 234, 0, 0, 0], test);
        let des = bincode::deserialize::<PeerEventDes>(&test).unwrap();
        assert_eq!(format!("{:?}", &des), format!("{:?}", &event));
    }
}
