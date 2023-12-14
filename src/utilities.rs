

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
pub fn combine_vectors(solution_vector: &mut Vec<(String, f64)>, vector1: Vec<String>, vector2: Vec<f64>) -> Result<(), String> {
    // Check if the vectors have the same length
    if vector1.len() != vector2.len() {
        return Err("Vectors have different lengths".to_string());
    }

    // Clear the current contents
    solution_vector.clear();

    // Combining the vectors
    for (ts, data) in vector1.into_iter().zip(vector2.into_iter()) {
        solution_vector.push((ts, data));
    }

    Ok(())
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

