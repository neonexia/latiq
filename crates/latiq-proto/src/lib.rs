//! Generated gRPC contracts for the Control and Admin surfaces.
pub mod v1 {
    tonic::include_proto!("latiq.v1");
}

#[cfg(test)]
mod tests {
    #[test]
    fn types_are_generated() {
        let _ = super::v1::CreatePondAssignmentRequest {
            name: "incident-001".into(),
            owner_identity: "agent".into(),
            policy_json: "{}".into(),
        };
    }
}
