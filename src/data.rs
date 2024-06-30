// Datasouces are generator for data
//
// Datasources can be registered at Runtime

use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

use async_fn_traits::AsyncFn1;

/// RuntimeDataSources are used to store data generated at runtime for Execution.
#[derive(Debug, Default)]
pub struct RuntimeDataStore(HashMap<TypeId, Box<dyn Any + Send + Sync>>);

impl RuntimeDataStore {
    pub fn new() -> Self {
        Self(HashMap::default())
    }

    pub fn get<T: Any>(&self) -> Option<&T> {
        self.0
            .get(&std::any::TypeId::of::<T>())
            .and_then(|x| x.downcast_ref())
    }

    pub fn get_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.0
            .get_mut(&std::any::TypeId::of::<T>())
            .and_then(|x| x.downcast_mut())
    }

    pub fn clear(&mut self) {
        self.0.clear()
    }

    pub fn insert<V: Any + Sync + Send>(&mut self, v: V) -> Option<Box<V>> {
        self.0
            .insert(std::any::TypeId::of::<V>(), Box::new(v))
            .and_then(|x| x.downcast::<V>().ok())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn contains<T: Any>(&self) -> bool {
        self.0.contains_key(&TypeId::of::<T>())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[async_trait::async_trait]
pub trait DatastoreModifier: Sync {
    async fn init_store(&self, store: &mut RuntimeDataStore);
}

#[async_trait::async_trait]
impl<F> DatastoreModifier for F
where
    F: for<'a> AsyncFn1<&'a mut RuntimeDataStore, Output = ()> + Sync,
    for<'b> <F as AsyncFn1<&'b mut RuntimeDataStore>>::OutputFuture: Send,
{
    async fn init_store(&self, store: &mut RuntimeDataStore) {
        self(store).await
    }
}

/*
/// Implemented on Types that
pub trait Extractor<'a>: Sized {
    fn from_runtime(runtime: &'a RuntimeDataStore) -> Result<Self, Error>;
}

impl<'a, T: 'static> Extractor<'a> for &'a T {
    fn from_runtime(runtime: &'a RuntimeDataStore) -> Result<Self, Error> {
        runtime
            .get::<T>()
            .ok_or_else(|| Error::termination("value not found in the datastore"))
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

 */
