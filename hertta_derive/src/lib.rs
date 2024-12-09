use proc_macro::TokenStream;
use quote::quote;

#[proc_macro_derive(Members)]
pub fn members_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_members(&ast)
}

fn impl_members(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl Members for #name {
            fn members(&self) -> &Vec<String> {
                &self.members
            }
            fn members_mut(&mut self) -> &mut Vec<String> {
                &mut self.members
            }
        }
    };
    gen.into()
}

#[proc_macro_derive(Name)]
pub fn name_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_name(&ast)
}

fn impl_name(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl Name for #name {
            fn name(&self) -> &String {
                &self.name
            }
        }
    };
    gen.into()
}
