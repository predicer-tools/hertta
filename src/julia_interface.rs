use jlrs::data::managed::function::Function;
use jlrs::data::managed::union_all::UnionAll;
use jlrs::data::managed::value::ValueData;
use jlrs::data::managed::value::ValueResult;
use jlrs::memory::target::ExtendedTarget;
use jlrs::prelude::*;
use jlrs::memory::target::frame;

fn prepare_callable<'target, 'data, T: Target<'target>>(
    target: T,
    function: &[&str],
) -> Function<'target, 'data> {
    unsafe {
        let mut module = Module::main(&target);
        for submodule in &function[0..function.len() - 1] {
            module = module
                .submodule(&target, submodule)
                .expect(&format!("error with module {}", &submodule))
                .as_managed();
        }
        let function_name = function.last().expect("function name missing");
        module
            .function(&target, function_name)
            .expect(&format!("error with function {}", &function_name))
            .as_managed()
    }
}

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

// Convert a slice of pairs of strings and f64's to an `OrderedDict`
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

pub fn make_rust_vector_f64<'target, 'data>(frame: &mut frame::GcFrame<'target>, vector: &Value<'target, 'data>) -> Vec<f64> {
    let length = unsafe {
        Module::base(&frame).function(&frame, "length").unwrap().as_managed()
    };
    let vector_length = unsafe {
        length.call1(&frame, *vector).unwrap().as_managed().unbox::<i64>().unwrap()
    };
    let get_index = unsafe {
        Module::base(&frame).function(&frame, "getindex").unwrap().as_managed()
    };

    let mut rust_vector: Vec<f64> = Vec::new();

    for n in 1..vector_length + 1 {
        frame.scope(|mut frame| {
            let index = Value::new(&mut frame, n);
            let x = unsafe {
                get_index.call2(&mut frame, *vector, index).into_jlrs_result().unwrap().unbox::<f64>().unwrap()
            };
            rust_vector.push(x);
            Ok(())
        }).unwrap();
    }

    return rust_vector
}

pub fn make_rust_vector_string<'target, 'data>(frame: &mut frame::GcFrame<'target>, vector: &Value<'target, 'data>) -> Vec<String> {
    let length = unsafe {
        Module::base(&frame).function(&frame, "length").unwrap().as_managed()
    };
    let vector_length = unsafe {
        length.call1(&frame, *vector).unwrap().as_managed().unbox::<i64>().unwrap()
    };
    let get_index = unsafe {
        Module::base(&frame).function(&frame, "getindex").unwrap().as_managed()
    };

    let mut rust_vector: Vec<String> = Vec::new();

    for n in 1..vector_length + 1 {
        frame.scope(|mut frame| {
            let index = Value::new(&mut frame, n);
            let x = unsafe {
                get_index.call2(&mut frame, *vector, index).into_jlrs_result().unwrap().unbox::<String>().unwrap()
            };
            match x {
                Ok(s) => {
                    rust_vector.push(s);
                }
                Err(error) => println!("Error converting to string: {:?}", error),
            }
            Ok(())
        }).unwrap();
    }

    return rust_vector
}




