

/// Prints each element of a generic vector to the console.
///
/// This function is generic over the element type, requiring that each element implement
/// the `std::fmt::Display` trait for formatting.
///
/// # Arguments
///
/// - `vec`: A reference to the vector containing elements of type `T`.
///
pub fn _print_vector<T: std::fmt::Display>(vec: &Vec<T>) {
    for item in vec.iter() {
        println!("{}", item);
    }
}

/// Prints each element of a vector of tuples `(String, f64)` to the console.
///
/// Specifically formatted to handle a vector of tuples, where each tuple consists of
/// a `String` and an `f64`, printing them in a `name: value` format.
///
/// # Arguments
///
/// - `v`: A reference to the vector containing tuples of `(String, f64)`.
///
pub fn _print_tuple_vector(v: &Vec<(String, f64)>) {
    for (name, value) in v {
        println!("{}: {}", name, value);
    }
}

/// Combines two vectors into a single vector of tuples.
///
/// The function checks if both input vectors are of the same length and returns an error if they are not.
///
/// # Arguments
///
/// - `solution_vector`: A mutable reference to the vector that will store the combined tuples.
///   This vector is cleared at the beginning of the function to ensure it only contains the 
///   combined results.
/// - `vector1`: A vector of `String` elements, each representing the first element of the tuple.
/// - `vector2`: A vector of `f64` elements, each representing the second element of the tuple.
///
/// # Errors
///
/// Returns an `Err` with a string message if `vector1` and `vector2` have different lengths.
///
pub fn combine_vectors(vector1: Vec<String>, vector2: Vec<f64>) -> Result<Vec<(String, f64)>, String> {
    if vector1.len() != vector2.len() {
        return Err("Vectors have different lengths".to_string());
    }

    let combined_vector: Vec<(String, f64)> = vector1.into_iter().zip(vector2.into_iter()).collect();

    Ok(combined_vector)
}


/// Checks if a string is a valid HTTP header value.
///
/// This function validates a given string to see if it is suitable to be used as an HTTP header value.
/// The criteria for this example includes being an ASCII string and not containing any control characters.
///
/// # Arguments
///
/// - `value`: A string slice reference that is to be validated.
///
/// # Returns
///
/// Returns `true` if the string is ASCII and does not contain control characters, otherwise `false`.
///
pub fn is_valid_http_header_value(value: &str) -> bool {
    value.is_ascii() && !value.chars().any(|ch| ch.is_control())
}

#[cfg(test)]
mod tests {
    use super::*;

    /* 
    #[test]
    fn test_combine_vectors_success() {
        let vector1 = vec!["a".to_string(), "b".to_string()];
        let vector2 = vec![1.0, 2.0];

        assert!(combine_vectors(vector1, vector2).is_ok());
        assert_eq!(solution_vector, vec![("a".to_string(), 1.0), ("b".to_string(), 2.0)]);
    }
    

    #[test]
    fn test_combine_vectors_different_length() {
        let vector1 = vec!["a".to_string()];
        let vector2 = vec![1.0, 2.0];

        assert!(combine_vectors(vector1, vector2).is_err());
    }

    #[test]
    fn test_combine_vectors_empty() {
        let mut solution_vector = Vec::new();
        let vector1: Vec<String> = Vec::new();
        let vector2: Vec<f64> = Vec::new();

        assert!(combine_vectors(&mut solution_vector, vector1, vector2).is_ok());
        assert!(solution_vector.is_empty());
    }

    */
}


