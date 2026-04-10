/*!
# cuda-mesh

Service mesh for agent fleets.

Agents aren't isolated. They form a mesh — discovering each other,
balancing load, routing traffic, and checking health. This crate
provides the connective tissue between fleet members.

- Service discovery (register, lookup, deregister)
- Load balancing (round-robin, random, least-connections, weighted)
- Health checking (active + passive)
- Traffic routing with versioning
- Circuit integration
*/

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A service instance
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceInstance {
    pub id: String,
    pub name: String,
    pub address: String,
    pub port: u16,
    pub version: String,
    pub weight: f64,
    pub healthy: bool,
    pub active_connections: u32,
    pub metadata: HashMap<String, String>,
    pub registered_ms: u64,
    pub last_heartbeat_ms: u64,
}

/// Load balancing strategy
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LBStrategy { RoundRobin, Random, LeastConnections, Weighted }

/// A route rule
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RouteRule {
    pub service: String,
    pub version: Option<String>,
    pub weight: f64,       // traffic percentage
    pub header_match: Option<String>,
}

/// Health status
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceHealth { Healthy, Degraded, Unhealthy, Unknown }

/// Service registry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceRegistry {
    instances: HashMap<String, ServiceInstance>,
    services: HashMap<String, Vec<String>>, // service_name → [instance_ids]
    rr_counters: HashMap<String, usize>,    // round-robin state
}

impl ServiceRegistry {
    pub fn new() -> Self { ServiceRegistry { instances: HashMap::new(), services: HashMap::new(), rr_counters: HashMap::new() } }

    /// Register a service instance
    pub fn register(&mut self, instance: ServiceInstance) {
        let id = instance.id.clone();
        let name = instance.name.clone();
        self.services.entry(name).or_insert_with(Vec::new).push(id.clone());
        self.instances.insert(id, instance);
    }

    /// Deregister
    pub fn deregister(&mut self, instance_id: &str) {
        if let Some(inst) = self.instances.remove(instance_id) {
            if let Some(ids) = self.services.get_mut(&inst.name) {
                ids.retain(|i| i != instance_id);
            }
        }
    }

    /// Heartbeat
    pub fn heartbeat(&mut self, instance_id: &str) {
        if let Some(inst) = self.instances.get_mut(instance_id) {
            inst.last_heartbeat_ms = now();
            inst.healthy = true;
        }
    }

    /// Lookup service instances
    pub fn lookup(&self, service: &str, version: Option<&str>) -> Vec<&ServiceInstance> {
        let ids = match self.services.get(service) { Some(ids) => ids, None => return vec![] };
        ids.iter().filter_map(|id| self.instances.get(id))
            .filter(|i| i.healthy)
            .filter(|i| version.map_or(true, |v| v == i.version))
            .collect()
    }

    /// Mark unhealthy instances
    pub fn check_health(&mut self, timeout_ms: u64) -> u32 {
        let now = now();
        let mut unhealthy = 0u32;
        for inst in self.instances.values_mut() {
            let elapsed = now - inst.last_heartbeat_ms;
            if elapsed > timeout_ms { inst.healthy = false; unhealthy += 1; }
        }
        unhealthy
    }

    /// Service health summary
    pub fn service_health(&self, service: &str) -> ServiceHealth {
        let instances = self.lookup(service, None);
        if instances.is_empty() { return ServiceHealth::Unknown; }
        let healthy = instances.iter().filter(|i| i.healthy).count();
        let ratio = healthy as f64 / instances.len() as f64;
        if ratio > 0.8 { ServiceHealth::Healthy }
        else if ratio > 0.4 { ServiceHealth::Degraded }
        else { ServiceHealth::Unhealthy }
    }

    pub fn total_instances(&self) -> usize { self.instances.len() }
}

/// Load balancer
pub struct LoadBalancer {
    registry: ServiceRegistry,
    strategy: LBStrategy,
}

impl LoadBalancer {
    pub fn new(registry: ServiceRegistry, strategy: LBStrategy) -> Self { LoadBalancer { registry, strategy } }

    /// Select an instance for a service
    pub fn select(&mut self, service: &str) -> Option<String> {
        let instances = self.registry.lookup(service, None);
        if instances.is_empty() { return None; }

        match self.strategy {
            LBStrategy::RoundRobin => {
                let counter = self.registry.rr_counters.entry(service.to_string()).or_insert(0);
                let idx = *counter % instances.len();
                *counter += 1;
                Some(instances[idx].id.clone())
            }
            LBStrategy::LeastConnections => {
                instances.iter().min_by_key(|i| i.active_connections).map(|i| i.id.clone())
            }
            LBStrategy::Weighted => {
                let total_weight: f64 = instances.iter().map(|i| i.weight).sum();
                let mut rand = (now() as f64 / 1000.0 % 1.0) * total_weight;
                for inst in &instances {
                    rand -= inst.weight;
                    if rand <= 0.0 { return Some(inst.id.clone()); }
                }
                instances.last().map(|i| i.id.clone())
            }
            LBStrategy::Random => {
                if instances.is_empty() { return None; }
                let idx = (now() as usize) % instances.len();
                Some(instances[idx].id.clone())
            }
        }
    }
}

/// Summary
pub fn mesh_summary(registry: &ServiceRegistry) -> String {
    let services = registry.services.len();
    let instances = registry.total_instances();
    let healthy: Vec<ServiceHealth> = registry.services.keys().map(|s| registry.service_health(s)).collect();
    let healthy_count = healthy.iter().filter(|h| **h == ServiceHealth::Healthy).count();
    format!("Mesh: {} services, {} instances, {}/{} healthy", services, instances, healthy_count, services)
}

fn now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_instance(id: &str, name: &str, version: &str) -> ServiceInstance {
        ServiceInstance { id: id.to_string(), name: name.to_string(), address: "127.0.0.1".into(), port: 8080, version: version.to_string(), weight: 1.0, healthy: true, active_connections: 0, metadata: HashMap::new(), registered_ms: now(), last_heartbeat_ms: now() }
    }

    #[test]
    fn test_register_and_lookup() {
        let mut reg = ServiceRegistry::new();
        reg.register(make_instance("i1", "api", "v1"));
        reg.register(make_instance("i2", "api", "v1"));
        let found = reg.lookup("api", None);
        assert_eq!(found.len(), 2);
    }

    #[test]
    fn test_version_filter() {
        let mut reg = ServiceRegistry::new();
        reg.register(make_instance("i1", "api", "v1"));
        reg.register(make_instance("i2", "api", "v2"));
        let v1 = reg.lookup("api", Some("v1"));
        assert_eq!(v1.len(), 1);
    }

    #[test]
    fn test_deregister() {
        let mut reg = ServiceRegistry::new();
        reg.register(make_instance("i1", "api", "v1"));
        reg.deregister("i1");
        assert_eq!(reg.lookup("api", None).len(), 0);
    }

    #[test]
    fn test_round_robin() {
        let mut reg = ServiceRegistry::new();
        reg.register(make_instance("i1", "api", "v1"));
        reg.register(make_instance("i2", "api", "v1"));
        let mut lb = LoadBalancer::new(reg, LBStrategy::RoundRobin);
        let first = lb.select("api").unwrap();
        let second = lb.select("api").unwrap();
        let third = lb.select("api").unwrap();
        assert_eq!(first, third); // cycles back
        assert_ne!(first, second); // different
    }

    #[test]
    fn test_least_connections() {
        let mut reg = ServiceRegistry::new();
        let mut i1 = make_instance("i1", "api", "v1");
        i1.active_connections = 5;
        let mut i2 = make_instance("i2", "api", "v1");
        i2.active_connections = 1;
        reg.register(i1); reg.register(i2);
        let mut lb = LoadBalancer::new(reg, LBStrategy::LeastConnections);
        assert_eq!(lb.select("api").unwrap(), "i2");
    }

    #[test]
    fn test_weighted_selection() {
        let mut reg = ServiceRegistry::new();
        let mut i1 = make_instance("i1", "api", "v1"); i1.weight = 10.0;
        let mut i2 = make_instance("i2", "api", "v1"); i2.weight = 1.0;
        reg.register(i1); reg.register(i2);
        let mut lb = LoadBalancer::new(reg, LBStrategy::Weighted);
        // i1 should be selected much more often
        let mut i1_count = 0u32;
        for _ in 0..100 { if lb.select("api").unwrap() == "i1" { i1_count += 1; } }
        assert!(i1_count > 50);
    }

    #[test]
    fn test_health_check_timeout() {
        let mut reg = ServiceRegistry::new();
        let mut inst = make_instance("i1", "api", "v1");
        inst.last_heartbeat_ms = 0;
        reg.register(inst);
        let unhealthy = reg.check_health(1000);
        assert_eq!(unhealthy, 1);
    }

    #[test]
    fn test_service_health_summary() {
        let mut reg = ServiceRegistry::new();
        reg.register(make_instance("i1", "api", "v1"));
        reg.register(make_instance("i2", "api", "v1"));
        assert_eq!(reg.service_health("api"), ServiceHealth::Healthy);
    }

    #[test]
    fn test_unknown_service_health() {
        let reg = ServiceRegistry::new();
        assert_eq!(reg.service_health("nonexistent"), ServiceHealth::Unknown);
    }

    #[test]
    fn test_mesh_summary() {
        let reg = ServiceRegistry::new();
        let s = mesh_summary(&reg);
        assert!(s.contains("0 services"));
    }
}
