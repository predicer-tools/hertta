use super::MaybeError;
use crate::input_data::Name;
use crate::input_data_base::{GroupMember, Members, NamedGroup, NodeGroup, ProcessGroup, TypeName};

pub fn create_node_group(
    name: String,
    node_groups: &mut Vec<NodeGroup>,
    process_groups: &Vec<ProcessGroup>,
) -> MaybeError {
    create_group(name, node_groups, process_groups)
}

pub fn create_process_group(
    name: String,
    process_groups: &mut Vec<ProcessGroup>,
    node_groups: &Vec<NodeGroup>,
) -> MaybeError {
    create_group(name, process_groups, node_groups)
}

fn create_group<G: Name + NamedGroup + TypeName, H: Name + NamedGroup + TypeName>(
    name: String,
    target_groups: &mut Vec<G>,
    other_groups: &Vec<H>,
) -> MaybeError {
    let maybe_error = validate_name(&name, target_groups, other_groups);
    if maybe_error.message.is_some() {
        return maybe_error;
    }
    target_groups.push(G::new(name));
    MaybeError::new_ok()
}

fn validate_name<G: Name + TypeName, H: Name + TypeName>(
    name: &String,
    groups: &Vec<G>,
    other_groups: &Vec<H>,
) -> MaybeError {
    if name.is_empty() {
        return "name is empty".into();
    }
    if groups.iter().any(|g| *g.name() == *name) {
        return format!("a {} with the same name exists", G::type_name()).into();
    }
    if other_groups.iter().any(|g| *g.name() == *name) {
        return format!("a {} with the same name exists", H::type_name()).into();
    }
    MaybeError::new_ok()
}

pub fn add_to_group<M: GroupMember + Name + TypeName, G: Members + Name>(
    item_name: &str,
    group_name: &str,
    items: &mut Vec<M>,
    groups: &mut Vec<G>,
) -> MaybeError {
    let item = match items.iter_mut().find(|n| n.name() == item_name) {
        Some(node) => node,
        None => return format!("no such {}", M::type_name()).into(),
    };
    if item.groups().iter().any(|g| g == group_name) {
        return format!("{} is in the group already", M::type_name()).into();
    }
    let group = match groups.iter_mut().find(|g| g.name() == group_name) {
        Some(group) => group,
        None => return "no such group".into(),
    };
    item.groups_mut().push(group_name.into());
    group.members_mut().push(item_name.into());
    MaybeError::new_ok()
}

pub fn delete_group(
    name: &str,
    node_groups: &mut Vec<NodeGroup>,
    process_groups: &mut Vec<ProcessGroup>,
) -> MaybeError {
    if let Some(group_position) = node_groups.iter().position(|g| g.name() == name) {
        node_groups.swap_remove(group_position);
        return MaybeError::new_ok();
    } else {
        let group_position = match process_groups.iter().position(|g| g.name() == name) {
            Some(position) => position,
            None => return "no such group".into(),
        };
        process_groups.swap_remove(group_position);
        return MaybeError::new_ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input_data_base::BaseNode;
    #[test]
    fn add_to_group_adds_member_to_group() {
        let mut items = vec![BaseNode::new("my node".into())];
        let mut groups = vec![NodeGroup::new("nodes".into())];
        let maybe_error = add_to_group("my node", "nodes", &mut items, &mut groups);
        assert!(maybe_error.message.is_none());
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].groups, vec![String::from("nodes")]);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].members, vec![String::from("my node")]);
    }
    #[test]
    fn create_node_group_complains_when_name_clashes_with_process_group() {
        let mut node_groups = Vec::new();
        let process_groups = vec![ProcessGroup::new("my group".into())];
        let maybe_error = create_node_group("my group".into(), &mut node_groups, &process_groups);
        match maybe_error.message {
            Some(error) => assert_eq!(error, "a process group with the same name exists"),
            None => panic!("creating node group should not succeed"),
        }
    }
    #[test]
    fn create_process_group_complains_when_name_clashes_with_node_group() {
        let mut process_groups = Vec::new();
        let node_groups = vec![NodeGroup::new("my group".into())];
        let maybe_error =
            create_process_group("my group".into(), &mut process_groups, &node_groups);
        match maybe_error.message {
            Some(error) => assert_eq!(error, "a node group with the same name exists"),
            None => panic!("creating process group should not succeed"),
        }
    }
    #[test]
    fn delete_node_group() {
        let mut node_groups = vec![NodeGroup::new("my group".into())];
        let mut process_groups = Vec::new();
        let maybe_error = delete_group("my group", &mut node_groups, &mut process_groups);
        if maybe_error.message.is_some() {
            panic!("deleting group should succeed");
        }
        assert!(node_groups.is_empty())
    }
    #[test]
    fn delete_process_group() {
        let mut node_groups = Vec::new();
        let mut process_groups = vec![ProcessGroup::new("my group".into())];
        let maybe_error = delete_group("my group", &mut node_groups, &mut process_groups);
        if maybe_error.message.is_some() {
            panic!("deleting group should succeed");
        }
        assert!(process_groups.is_empty())
    }
    #[test]
    fn delete_group_fails_with_non_existent_group() {
        let mut node_groups = Vec::new();
        let mut process_groups = Vec::new();
        let maybe_error = delete_group("my group", &mut node_groups, &mut process_groups);
        if let Some(error) = maybe_error.message {
            assert_eq!(error, "no such group");
        } else {
            panic!("deleting non-existent group should not succeed");
        }
    }
}
