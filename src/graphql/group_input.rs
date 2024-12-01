use super::MaybeError;
use crate::input_data::{Group, Name};
use crate::input_data_base::GroupMember;

pub fn add_node_group(name: String, groups: &mut Vec<Group>) -> MaybeError {
    add_group(name, "node", groups)
}

pub fn add_process_group(name: String, groups: &mut Vec<Group>) -> MaybeError {
    add_group(name, "process", groups)
}

fn add_group(name: String, group_type: &str, groups: &mut Vec<Group>) -> MaybeError {
    let maybe_error = validate_name(&name, &groups);
    if maybe_error.error.is_some() {
        return maybe_error;
    }
    groups.push(Group {
        name,
        g_type: String::from(group_type),
        members: Vec::new(),
    });
    MaybeError::new_ok()
}

fn validate_name(name: &String, groups: &Vec<Group>) -> MaybeError {
    if name.is_empty() {
        return "name is empty".into();
    }
    if groups.iter().find(|g| g.name == *name).is_some() {
        return "a group with the same name exists".into();
    }
    MaybeError::new_ok()
}

pub fn add_to_group<T: GroupMember + Name>(
    item_name: String,
    group_name: String,
    items: &mut Vec<T>,
    groups: &mut Vec<Group>,
) -> MaybeError {
    let item = match items.iter_mut().find(|n| n.name() == &item_name) {
        Some(node) => node,
        None => return "no such node".into(),
    };
    if item.groups().iter().find(|g| **g == group_name).is_some() {
        return "node is in the group already".into();
    }
    let group = match groups.iter_mut().find(|g| g.name == group_name) {
        Some(group) => group,
        None => return "no such group".into(),
    };
    if group.g_type != T::group_type() {
        return MaybeError::from(format!("wrong target group type '{}'", group.g_type).as_str());
    }
    item.groups_mut().push(group_name.clone());
    group.members.push(group_name);
    MaybeError::new_ok()
}
