use rtb::bid_request::{Source, SupplyChain};

/// Extracts SupplyChain from an existing Source object,
/// taking ownership out of the object
pub fn take_schain(source: &mut Source) -> Option<SupplyChain> {
    source.schain.take().or_else(|| {
        #[allow(deprecated)]
        source.ext.as_mut().and_then(|ext| ext.schain.take())
    })
}

/// Resolves the SupplyChain from an optional Source
/// object, regardless of whether it exists in
/// source.schain or source.ext.schain
pub fn resolve_schain(source_opt: Option<&Source>) -> Option<&SupplyChain> {
    source_opt.and_then(|source| {
        source.schain.as_ref().or_else(|| {
            #[allow(deprecated)]
            source.ext.as_ref().and_then(|ext| ext.schain.as_ref())
        })
    })
}
