use super::MaybeError;
use crate::input_data::Name;
use crate::input_data_base::{GroupMember, Members, NamedGroup, NodeGroup, ProcessGroup};

pub fn create_node_group(name: String, groups: &mut Vec<NodeGroup>) -> MaybeError {
    create_group(name, groups)
}

pub fn create_process_group(name: String, groups: &mut Vec<ProcessGroup>) -> MaybeError {
    create_group(name, groups)
}

fn create_group<T: Name + NamedGroup>(name: String, groups: &mut Vec<T>) -> MaybeError {
    let maybe_error = validate_name(&name, &groups);
    if maybe_error.error.is_some() {
        return maybe_error;
    }
    groups.push(T::new(name));
    MaybeError::new_ok()
}

fn validate_name<T: Name>(name: &String, groups: &Vec<T>) -> MaybeError {
    if name.is_empty() {
        return "name is empty".into();
    }
    if groups.iter().find(|&g| *g.name() == *name).is_some() {
        return "a group with the same name exists".into();
    }
    MaybeError::new_ok()
}

pub fn add_to_group<M: GroupMember + Name, G: Members + Name>(
    item_name: &str,
    group_name: &str,
    items: &mut Vec<M>,
    groups: &mut Vec<G>,
) -> MaybeError {
    let item = match items.iter_mut().find(|n| n.name() == item_name) {
        Some(node) => node,
        None => return format!("no such {}", M::group_type()).into(),
    };
    if item.groups().iter().find(|&g| *g == group_name).is_some() {
        return format!("{} is in the group already", M::group_type()).into();
    }
    let group = match groups.iter_mut().find(|g| g.name() == group_name) {
        Some(group) => group,
        None => return "no such group".into(),
    };
    item.groups_mut().push(group_name.into());
    group.members_mut().push(item_name.into());
    MaybeError::new_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input_data_base::BaseNode;
    #[test]
    fn add_to_group_adds_member_to_group() {
        let mut items = vec![BaseNode::with_name("my node".into())];
        let mut groups = vec![NodeGroup::new("nodes".into())];
        let maybe_error = add_to_group("my node", "nodes", &mut items, &mut groups);
        assert!(maybe_error.error.is_none());
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].groups, vec![String::from("nodes")]);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].members, vec![String::from("my node")]);
    }
}
