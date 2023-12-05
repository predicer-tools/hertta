


pub fn _print_vector<T: std::fmt::Display>(vec: &Vec<T>) {
    for item in vec.iter() {
        println!("{}", item);
    }
}

pub fn _print_tuple_vector(v: &Vec<(String, f64)>) {
    for (name, value) in v {
        println!("{}: {}", name, value);
    }
}

pub fn print_f64_vector(vector: &Vec<f64>) {
    for value in vector.iter() {
        println!("{}", value);
    }
}


pub fn combine_vectors(solution_vector: &mut Vec<(String, f64)>, ts_vector: Vec<String>, data_vector: Vec<f64>) {
    // Clear the current contents
    solution_vector.clear();

    // Assuming ts_vector and data_vector are of the same length
    for (ts, data) in ts_vector.into_iter().zip(data_vector.into_iter()) {
        solution_vector.push((ts, data));
    }
}

