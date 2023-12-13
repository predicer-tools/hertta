use jlrs::data::managed::function::Function;
use jlrs::data::managed::union_all::UnionAll;
use jlrs::data::managed::value::ValueData;
use jlrs::data::managed::value::ValueResult;
use jlrs::memory::target::ExtendedTarget;
use jlrs::prelude::*;
use jlrs::memory::target::frame;

/// This file contains helper functions that allow Julia functions to be used using Rust. 
/// This is implemented with the help of the jlrs library.

/// Prepares a callable Julia function using Rust.
///
/// This function allows Rust code to interact with Julia functions by leveraging the `jlrs` library.
/// It navigates through Julia modules and submodules to locate the specified Julia function
/// and makes it callable from Rust.
///
/// # Arguments
///
/// - `target`: The Julia environment where the function resides.
/// - `function`: A slice of string slices representing the hierarchical path to the target Julia function.
///   The last element is assumed to be the function name, with preceding elements representing modules and submodules.
///
/// # Returns
///
/// Returns a `Function<'target, 'data>` object, which is a callable reference to the specified Julia function.
///
/// # Safety
///
/// This function contains unsafe code blocks and should be used with caution. 
///


fn prepare_callable<'target, 'data, T: Target<'target>>(
    target: T,
    function: &[&str],
) -> Function<'target, 'data> {
    unsafe {
        let mut module = Module::main(&target);
        for submodule in &function[0..function.len() - 1] {
            module = module
                .submodule(&target, submodule)
                .expect(&format!("Error with module {}", &submodule))
                .as_managed();
        }
        let function_name = function.last().expect("Function name is missing");
        module
            .function(&target, function_name)
            .expect(&format!("Error with function {}", &function_name))
            .as_managed()
    }
}

/// Calls a Julia function from Rust.
///
/// Facilitates calling a specified Julia function using the `jlrs` library, handling functions 
/// with or without arguments. It utilizes `prepare_callable` to locate and prepare the Julia function.
///
/// # Arguments
///
/// - `target`: The Julia environment in which the function is to be called.
/// - `function`: A slice of string slices representing the path to the target Julia function.
///   The last element is the function name, with preceding elements representing modules and submodules.
/// - `args`: A slice of `Value<'_, 'data>` representing the arguments to be passed to the Julia function.
///
/// # Returns
///
/// Returns a `ValueResult<'target, 'data, T>`, encapsulating the result of the Julia function call.
/// This can be either a successful return value or an error.
///
/// # Safety
///
/// Contains unsafe code due to direct calls to Julia functions. The caller must ensure that the
/// provided `target`, `function` path, and `args` are valid and that the Julia environment is properly initialized.
///
/// 
pub fn call<'target, 'data, T: Target<'target>>(
    target: T,
    function: &[&str],
    args: &[Value<'_, 'data>],
) -> ValueResult<'target, 'data, T> {
    let callable = prepare_callable(&target, &function);
    unsafe {
        if args.len() == 0 {
            callable.call0(target)
        } else {
            callable.call(target, args)
        }
    }
}

/// Activates a Julia project and instantiates its environment.
///
/// This function is designed to activate a specific Julia project located in the provided directory 
/// and to instantiate its environment using the Julia Package Manager (Pkg). It facilitates the 
/// management of project-specific dependencies in a Rust-Julia integrated setup.
///
/// # Arguments
///
/// - `target`: The Julia environment where the project activation and instantiation are to be executed.
/// - `project_dir`: A `Value<'_, 'data>` representing the directory of the Julia project.
///
/// # Returns
///
/// Returns a `Result<(), String>`. On successful execution, it returns `Ok(())`. If any operation fails,
/// it may return an `Err` with an error message.
///
/// # Safety
///
/// Contains unsafe code due to the evaluation of Julia strings and direct calls to Julia functions.
/// The caller is responsible for ensuring that the `target` and `project_dir` are valid and that 
/// the Julia environment is properly initialized.
///
pub fn activate_julia_project<'target, 'data, T: Target<'target>>(
    target: T,
    project_dir: Value<'_, 'data>,
) -> Result<(), String> {
    unsafe {
        Value::eval_string(&target, "using Pkg").unwrap();
    }
    call(&target, &["Pkg", "activate"], &[project_dir]).unwrap();
    call(&target, &["Pkg", "instantiate"], &[]).unwrap();
    Ok(())
}

/// Converts a slice of pairs (String, f64) into a Julia `OrderedDict`.
///
/// The function handles the creation and initialization of the `OrderedDict`
/// and inserts each key-value pair from the Rust data into the Julia dictionary.
///
/// # Arguments
///
/// - `target`: An `ExtendedTarget` that provides both a target for the result and a frame for
///   temporary data, ensuring proper memory management in the Julia environment.
/// - `data`: A slice of tuples, each containing a `String` (as the key) and an `f64` (as the value).
///
/// # Returns
///
/// Returns a `JlrsResult<ValueData<'target, 'static, T>>`, which encapsulates the constructed 
/// `OrderedDict` in the Julia environment. The result can be either a successful reference to the 
/// created `OrderedDict` or an error if the process fails at any point.
///

pub fn to_ordered_dict<'target, T>(
    target: ExtendedTarget<'target, '_, '_, T>,
    data: &[(String, f64)],
) -> JlrsResult<ValueData<'target, 'static, T>>
where
    T: Target<'target>,
{
    // An extended target provides a target for the result we want to return and a frame for
    // temporary data.
    let (target, frame) = target.split();
    frame.scope(|mut frame| {
        let ordered_dict = Module::main(&frame).global(&mut frame, "OrderedDict");
        let ordered_dict_ua = match ordered_dict {
            Ok(ordered_dict) => ordered_dict,
            Err(_) => {
                // Safety: using this package is fine.
                unsafe {
                    // Predicer depends on DataStructures which in turn depends on OrderedCollections
                    // which contains OrderedDict.
                    Value::eval_string(&mut frame, "using DataStructures").into_jlrs_result()?
                };
                Module::main(&frame).global(&mut frame, "OrderedDict")?
            }
        }
        .cast::<UnionAll>()?;
        // The key and value type.
        let types = [
            DataType::string_type(&frame).as_value(),
            DataType::float64_type(&frame).as_value(),
        ];
        // Apply the types to the OrderedDict UnionAll to create the OrderedDict{String, Int32}
        // DataType, and call its constructor.
        //
        // Safety: the types are correct and the constructor doesn't access any data that might
        // be in use.
        let ordered_dict = unsafe {
            let ordered_dict_ty = ordered_dict_ua
                .apply_types(&mut frame, types)
                .into_jlrs_result()?;
            ordered_dict_ty.call0(&mut frame).into_jlrs_result()?
        };
        let setindex_fn = Module::base(&target).function(&mut frame, "setindex!")?;
        for (key, value) in data {
            // Create the keys and values in temporary scopes to avoid rooting an arbitrarily
            // large number of pairs in the current frame.
            frame.scope(|mut frame| {
                let key = JuliaString::new(&mut frame, key).as_value();
                let value = Value::new(&mut frame, *value);
                // Safety: the ordered dict can only be used in this function until it is
                // returned, setindex! is a safe function.
                unsafe {
                    setindex_fn
                        .call3(&mut frame, ordered_dict, value, key)
                        .into_jlrs_result()?;
                }
                Ok(())
            })?;
        }
        Ok(ordered_dict.root(target))
    })
}

/// Converts a Julia array into a Rust `Vec<f64>`.
///
/// # Arguments
///
/// - `frame`: A mutable reference to a `GcFrame` from the jlrs crate, which provides a context for
///   Julia's garbage collector.
/// - `vector`: A reference to the Julia array (as `Value`) that needs to be converted.
///
/// # Returns
///
/// Returns a `Vec<f64>` which is a Rust vector containing the elements of the Julia array.
///

pub fn make_rust_vector_f64<'target, 'data>(
    frame: &mut frame::GcFrame<'target>,
    vector: &Value<'target, 'data>
) -> Vec<f64> {
    // Obtaining the length function from the base module of Julia.
    let length = unsafe {
        Module::base(&frame).function(&frame, "length").unwrap().as_managed()
    };

    // Calling the length function on the Julia array to get its length.
    let vector_length = unsafe {
        length.call1(&frame, *vector).unwrap().as_managed().unbox::<i64>().unwrap()
    };

    // Obtaining the getindex function from the base module of Julia.
    let get_index = unsafe {
        Module::base(&frame).function(&frame, "getindex").unwrap().as_managed()
    };

    let mut rust_vector: Vec<f64> = Vec::new();

    for n in 1..vector_length + 1 {
        // Using a scoped frame to manage memory for temporary values.
        frame.scope(|mut frame| {

            let index = Value::new(&mut frame, n);

            // Retrieving the element at the given index from the Julia array.
            let x = unsafe {
                get_index.call2(&mut frame, *vector, index)
                    .into_jlrs_result().unwrap()
                    .unbox::<f64>().unwrap()
            };

            rust_vector.push(x);

            Ok(())
        }).unwrap();
    }

    return rust_vector
}


/// Converts a Julia array into a Rust `Vec<String>`.
///
/// This function takes a Julia array (represented as `Value`) and converts it into a Rust vector
/// of `String`. It is designed to work with arrays of strings in Julia and transfer them into
/// Rust's native string data structure.
///
/// # Arguments
///
/// - `frame`: A mutable reference to a `GcFrame` from the jlrs crate, providing a context for 
///   Julia's garbage collector.
/// - `vector`: A reference to the Julia array (as `Value`) that needs to be converted.
///
/// # Returns
///
/// Returns a `Vec<String>`, a Rust vector containing the elements of the Julia array as strings.
///

pub fn make_rust_vector_string<'target, 'data>(
    frame: &mut frame::GcFrame<'target>,
    vector: &Value<'target, 'data>
) -> Vec<String> {
    // Obtaining the length function from Julia's base module.
    let length = unsafe {
        Module::base(&frame).function(&frame, "length").unwrap().as_managed()
    };

    // Calling the length function on the Julia array to determine its length.
    let vector_length = unsafe {
        length.call1(&frame, *vector).unwrap().as_managed().unbox::<i64>().unwrap()
    };

    // Obtaining the getindex function from Julia's base module.
    let get_index = unsafe {
        Module::base(&frame).function(&frame, "getindex").unwrap().as_managed()
    };

    let mut rust_vector: Vec<String> = Vec::new();

    for n in 1..vector_length + 1 {
        // Using a scoped frame to manage memory for temporary values.
        frame.scope(|mut frame| {
            // Creating a new Value for the current index.
            let index = Value::new(&mut frame, n);

            // Retrieving the element at the current index from the Julia array.
            let x = unsafe {
                get_index.call2(&mut frame, *vector, index)
                    .into_jlrs_result().unwrap().unbox::<String>().unwrap()
            };

            // Handling the conversion result.
            match x {
                Ok(s) => {
                    // Pushing the successfully converted string into the Rust vector.
                    rust_vector.push(s);
                }
                Err(error) => {
                    // Printing an error message if the conversion fails.
                    println!("Error converting to string: {:?}", error);
                }
            }
            Ok(())
        }).unwrap();
    }

    return rust_vector
}





