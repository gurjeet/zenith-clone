pub struct CacheStringRepr<T> {
    value: T,
    string: once_cell::race::OnceBox<String>,
}

impl<T> CacheStringRepr<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            string: Default::default(),
        }
    }
}

impl<T: ToString> CacheStringRepr<T> {
    pub fn as_str(&self) -> &str {
        self.string.get_or_init(|| Box::new(self.value.to_string()))
    }
}

impl<T: ToString> std::ops::Deref for CacheStringRepr<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T: ToString> From<T> for CacheStringRepr<T> {
    fn from(value: T) -> Self {
        CacheStringRepr::new(value)
    }
}

impl<T: ToString> std::fmt::Display for CacheStringRepr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
