use proc_macro::TokenStream;
use quote::quote;

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
