use super::MaybeError;
use crate::input_data::Name;
use crate::input_data_base::TypeName;

pub fn delete_named<T: Name + TypeName>(name: &str, items: &mut Vec<T>) -> MaybeError {
    let position = match items.iter().position(|i| i.name() == name) {
        Some(position) => position,
        None => return format!("no such {}", T::type_name()).into(),
    };
    items.swap_remove(position);
    MaybeError::new_ok()
}
