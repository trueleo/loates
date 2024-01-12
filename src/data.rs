// Datasouces are generator for data
//
// Datasources can be registered at Runtime

use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

use crate::error::Error;

/// RuntimeDataSources are used to store data generated at runtime for Execution and Scenarios.
pub type RuntimeDataStore = HashMap<TypeId, Box<dyn Any + Send + Sync>>;

pub trait DatastoreModifier: Sync {
    fn init_store(&self, store: &mut RuntimeDataStore);
}

impl<F> DatastoreModifier for F
where
    F: Fn(&mut RuntimeDataStore) + Sync,
{
    fn init_store(&self, store: &mut RuntimeDataStore) {
        self(store)
    }
}

/// Implemented on Types that
pub trait Extractor<'a>: Sized {
    fn from_runtime(runtime: &'a RuntimeDataStore) -> Result<Self, Error>;
}

impl<'a, T: 'static> Extractor<'a> for &'a T {
    fn from_runtime(runtime: &'a RuntimeDataStore) -> Result<Self, Error> {
        if let Some(val) = runtime.get(&TypeId::of::<T>()) {
            val.downcast_ref::<T>()
                .ok_or_else(|| Error::new_generic("Could not downcast the variant"))
        } else {
            Err(Error::new_generic("{} not found in the datastore"))
        }
    }
}

macro_rules! impl_extractor {
    {$($param:ident)*} => {
        impl<'a, $($param,)*> Extractor<'a> for ($($param,)*)
            where $($param: Extractor<'a>),*
        {
            #[allow(unused_variables)]
            fn from_runtime(runtime: &'a RuntimeDataStore) -> Result<Self, Error> {
                Ok(($($param::from_runtime(runtime)?,)*))
            }
        }
    };
}

impl_extractor! {}
impl_extractor! { A }
impl_extractor! { A B }
impl_extractor! { A B C }
impl_extractor! { A B C D }
impl_extractor! { A B C D E }
impl_extractor! { A B C D E F }
impl_extractor! { A B C D E F G }
impl_extractor! { A B C D E F G H }
impl_extractor! { A B C D E F G H I }
impl_extractor! { A B C D E F G H I J }
impl_extractor! { A B C D E F G H I J K }
impl_extractor! { A B C D E F G H I J K L }
impl_extractor! { A B C D E F G H I J K L M }
impl_extractor! { A B C D E F G H I J K L M N }
impl_extractor! { A B C D E F G H I J K L M N O }
impl_extractor! { A B C D E F G H I J K L M N O P }
