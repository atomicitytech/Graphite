use convert_case::{Case, Casing};
use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{Error, FnArg, Ident, Item, ItemFn, ItemMod, ItemUse, Pat, Visibility};

pub fn editor_commands_impl(attr: TokenStream, module: ItemMod) -> syn::Result<TokenStream> {
	if !attr.is_empty() {
		return Err(Error::new(attr.span(), "#[editor_commands] takes no arguments"));
	}
	for attr in &module.attrs {
		if !attr.path().is_ident("doc") {
			return Err(Error::new(attr.span(), "the #[editor_commands] module may not have other attributes"));
		}
	}
	let Some((_, items)) = module.content else {
		return Err(Error::new(module.mod_token.span, "#[editor_commands] requires a module with an inline body"));
	};

	let mut imports: Vec<ItemUse> = Vec::new();
	let mut functions: Vec<ItemFn> = Vec::new();
	for item in items {
		match item {
			Item::Use(import) => imports.push(import),
			Item::Fn(function) => functions.push(function),
			other => return Err(Error::new(other.span(), "only `use` imports and command functions may appear in an #[editor_commands] module")),
		}
	}

	let mut variants = TokenStream::new();
	let mut stubs = TokenStream::new();
	let mut arms = TokenStream::new();

	for function in &functions {
		for attr in &function.attrs {
			if !attr.path().is_ident("doc") {
				return Err(Error::new(
					attr.span(),
					"command functions may not have attributes; anything that doesn't fit the `fn name(args…) -> Message` contract belongs in a plain impl block",
				));
			}
		}
		if !matches!(function.vis, Visibility::Inherited) {
			return Err(Error::new(
				function.span(),
				"command functions have no visibility modifier; the macro generates the public JS-facing stub",
			));
		}

		let signature = &function.sig;
		if let Some(receiver) = signature.receiver() {
			return Err(Error::new(receiver.span(), "command functions take no `self`; they are pure `args… -> Message` translations"));
		}
		if !signature.generics.params.is_empty() || signature.asyncness.is_some() || signature.unsafety.is_some() {
			return Err(Error::new(signature.span(), "command functions must be plain non-generic, non-async, safe functions"));
		}

		let docs = &function.attrs;
		let fn_name = &signature.ident;
		let variant = Ident::new(&fn_name.to_string().to_case(Case::Pascal), fn_name.span());
		let js_name = Ident::new(&fn_name.to_string().to_case(Case::Camel), fn_name.span());

		let mut param_names = Vec::new();
		let mut param_types = Vec::new();
		for parameter in &signature.inputs {
			let FnArg::Typed(pat_type) = parameter else { unreachable!("receiver is rejected above") };
			let Pat::Ident(pat_ident) = &*pat_type.pat else {
				return Err(Error::new(pat_type.span(), "command parameters must be plain identifiers"));
			};
			param_names.push(&pat_ident.ident);
			param_types.push(&*pat_type.ty);
		}

		let return_type = &signature.output;
		let body = &function.block;

		// Remote-control fork: command bodies may return `Result<Message, String>` instead of `Message` when they
		// validate their inputs (remote payloads deserialize successfully but can still violate invariants — see the
		// `TryFrom` emission below). A body is fallible iff its declared return type's outermost path is `Result`.
		let fallible = matches!(&signature.output, syn::ReturnType::Type(_, ty)
			if matches!(&**ty, syn::Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "Result")));

		let span = fn_name.span();
		variants.extend(quote_spanned! {span=>
			#(#docs)*
			#variant { #(#param_names: #param_types,)* },
		});
		// Web stubs run the body in-process: infallible bodies dispatch directly, fallible ones dispatch on `Ok` and
		// log-and-drop on `Err` (so a bad payload can never panic the editor, from JS or from the remote path)
		let web_stub_body = if fallible {
			quote_spanned! {span=>
				match (move || #return_type #body)() {
					Ok(message) => self.dispatch(message),
					Err(error) => log::error!(concat!("Rejected invalid ", stringify!(#js_name), " command: {}"), error),
				}
			}
		} else {
			quote_spanned! {span=> self.dispatch((move || #return_type #body)()) }
		};
		stubs.extend(quote_spanned! {span=>
			#(#docs)*
			#[cfg(not(feature = "native"))]
			#[wasm_bindgen(js_name = #js_name)]
			pub fn #fn_name(&self, #(#param_names: #param_types,)*) {
				#web_stub_body
			}
			#(#docs)*
			#[cfg(feature = "native")]
			#[wasm_bindgen(js_name = #js_name)]
			pub fn #fn_name(&self, #(#param_names: #param_types,)*) {
				self.send(EditorCommand::#variant { #(#param_names,)* })
			}
		});
		// Conversion arms produce `Result<Message, String>`: fallible bodies already do, infallible ones are wrapped.
		// Bodies run inside a closure so their early `return`s keep targeting the command body, not `try_from`
		let arm_body = if fallible {
			quote_spanned! {span=> (move || #return_type #body)() }
		} else {
			quote_spanned! {span=> Ok((move || #return_type #body)()) }
		};
		arms.extend(quote_spanned! {span=>
			EditorCommand::#variant { #(#param_names,)* } => #arm_body,
		});
	}

	Ok(quote! {
		#(
			#[cfg(feature = "editor")]
			#imports
		)*

		// Remote-control fork: both cfg gates below originally excluded the plain web/wasm build
		// (`any(feature = "native", not(target_family = "wasm"))`); they are widened so the enum and its
		// conversion also exist on wasm, where the relay client deserializes remote payloads into commands.
		#[derive(serde::Serialize, serde::Deserialize)]
		pub enum EditorCommand {
			#variants
		}

		// Remote-control fork: emitted as `TryFrom` (upstream emits `From`) because remote payloads are untrusted —
		// they can deserialize successfully yet violate invariants the JS path upholds by construction (undefined
		// modifier-key bits, wrong widget value shapes, out-of-range node ids). Command bodies that validate return
		// `Result<Message, String>`; the rest are wrapped in `Ok` above.
		#[cfg(feature = "editor")]
		impl TryFrom<EditorCommand> for Message {
			type Error = String;
			// Command bodies run inside immediately-invoked closures to preserve their early-`return` semantics
			#[allow(clippy::redundant_closure_call)]
			fn try_from(command: EditorCommand) -> Result<Self, Self::Error> {
				match command {
					#arms
				}
			}
		}

		#[cfg(target_family = "wasm")]
		#[wasm_bindgen]
		impl EditorWrapper {
			#stubs
		}
	})
}
