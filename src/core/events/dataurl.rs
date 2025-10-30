use anyhow::{Result, anyhow, bail};
use url::Url;

/// A URL builder that supports typed key-value pairs with finalization semantics.
///
/// DataUrl allows you to build URLs through a pipeline, adding typed parameters
/// along the way. Once finalized, the URL becomes immutable and can be extracted
/// with the chosen protocol (http/https).
///
/// # Example
/// ```
/// let mut url = DataUrl::new("example.com", "beacon")?;
/// url.add_string("auction_id", "abc123")?
///    .add_bool("won", true)?
///    .add_int("price", 150)?;
/// url.finalize();
/// let final_url = url.url(true)?; // https://example.com/beacon?auction_id=abc123&won=true&price=150
/// ```
#[derive(Clone, Debug)]
pub struct DataUrl {
    domain: String,
    path: String,
    url: Url,
    finalized: bool,
}

#[allow(dead_code)]
impl DataUrl {
    /// Creates a new DataUrl from a domain and path.
    ///
    /// The domain should not include a protocol (e.g., "example.com", not "https://example.com").
    /// Slashes are handled automatically - both "beacon" and "/beacon" are valid paths.
    ///
    /// # Arguments
    /// * `domain` - The domain without protocol (e.g., "example.com")
    /// * `path` - The path component (e.g., "beacon" or "/beacon")
    pub fn new(domain: &str, path: &str) -> Result<Self> {
        let domain = domain.trim_end_matches('/');
        let path = path.trim_start_matches('/');

        // Create internal URL with http for query manipulation
        let url_str = format!("https://{}/{}", domain, path);
        let url = Url::parse(&url_str)?;

        Ok(Self {
            domain: domain.to_string(),
            path: path.to_string(),
            url,
            finalized: false,
        })
    }

    /// Parses an existing URL string into a finalized DataUrl.
    ///
    /// This is useful for parsing beacon URLs that have already been constructed.
    /// The returned DataUrl is immediately finalized and cannot be mutated.
    pub fn from(url_str: &str) -> Result<Self> {
        let url = Url::parse(url_str)?;

        let domain = url
            .host_str()
            .ok_or_else(|| anyhow!("URL missing host"))?
            .to_string();

        let path = url.path().trim_start_matches('/').to_string();

        Ok(Self {
            domain,
            path,
            url,
            finalized: true,
        })
    }

    /// Adds a string parameter to the URL.
    ///
    /// # Errors
    /// Returns an error if the URL has already been finalized.
    pub fn add_string(&mut self, key: &str, value: &str) -> Result<&mut Self> {
        if self.finalized {
            bail!("Cannot add parameters to a finalized DataUrl");
        }
        self.url.query_pairs_mut().append_pair(key, value);
        Ok(self)
    }

    /// Adds a boolean parameter to the URL.
    ///
    /// # Errors
    /// Returns an error if the URL has already been finalized.
    pub fn add_bool(&mut self, key: &str, value: bool) -> Result<&mut Self> {
        if self.finalized {
            bail!("Cannot add parameters to a finalized DataUrl");
        }
        self.url
            .query_pairs_mut()
            .append_pair(key, &value.to_string());
        Ok(self)
    }

    /// Adds an integer parameter to the URL.
    ///
    /// # Errors
    /// Returns an error if the URL has already been finalized.
    pub fn add_int(&mut self, key: &str, value: i64) -> Result<&mut Self> {
        if self.finalized {
            bail!("Cannot add parameters to a finalized DataUrl");
        }
        self.url
            .query_pairs_mut()
            .append_pair(key, &value.to_string());
        Ok(self)
    }

    /// Adds a floating-point parameter to the URL.
    ///
    /// # Errors
    /// Returns an error if the URL has already been finalized.
    pub fn add_float(&mut self, key: &str, value: f64) -> Result<&mut Self> {
        if self.finalized {
            bail!("Cannot add parameters to a finalized DataUrl");
        }
        self.url
            .query_pairs_mut()
            .append_pair(key, &value.to_string());
        Ok(self)
    }

    /// Gets a string parameter from the URL.
    ///
    /// Returns `Ok(Some(value))` if the parameter exists,
    /// `Ok(None)` if the parameter is missing.
    pub fn get_string(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .url
            .query_pairs()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.to_string()))
    }

    /// Gets a boolean parameter from the URL.
    ///
    /// Returns `Ok(Some(value))` if the parameter exists and is a valid boolean,
    /// `Ok(None)` if the parameter is missing,
    /// `Err` if the parameter exists but cannot be parsed as a boolean.
    pub fn get_bool(&self, key: &str) -> Result<Option<bool>> {
        match self.url.query_pairs().find(|(k, _)| k == key) {
            Some((_, v)) => {
                let parsed = v.parse::<bool>().map_err(|e| {
                    anyhow!(
                        "Failed to parse '{}' as boolean for key '{}': {}",
                        v,
                        key,
                        e
                    )
                })?;
                Ok(Some(parsed))
            }
            None => Ok(None),
        }
    }

    /// Gets an integer parameter from the URL.
    ///
    /// Returns `Ok(Some(value))` if the parameter exists and is a valid integer,
    /// `Ok(None)` if the parameter is missing,
    /// `Err` if the parameter exists but cannot be parsed as an integer.
    pub fn get_int(&self, key: &str) -> Result<Option<i64>> {
        match self.url.query_pairs().find(|(k, _)| k == key) {
            Some((_, v)) => {
                let parsed = v.parse::<i64>().map_err(|e| {
                    anyhow!(
                        "Failed to parse '{}' as integer for key '{}': {}",
                        v,
                        key,
                        e
                    )
                })?;
                Ok(Some(parsed))
            }
            None => Ok(None),
        }
    }

    /// Gets a floating-point parameter from the URL.
    ///
    /// Returns `Ok(Some(value))` if the parameter exists and is a valid float,
    /// `Ok(None)` if the parameter is missing,
    /// `Err` if the parameter exists but cannot be parsed as a float.
    pub fn get_float(&self, key: &str) -> Result<Option<f64>> {
        match self.url.query_pairs().find(|(k, _)| k == key) {
            Some((_, v)) => {
                let parsed = v.parse::<f64>().map_err(|e| {
                    anyhow!("Failed to parse '{}' as float for key '{}': {}", v, key, e)
                })?;
                Ok(Some(parsed))
            }
            None => Ok(None),
        }
    }

    /// Gets a required string parameter from the URL.
    ///
    /// Returns `Ok(value)` if the parameter exists,
    /// `Err` if the parameter is missing.
    pub fn get_required_string(&self, key: &str) -> Result<String> {
        self.get_string(key)?
            .ok_or_else(|| anyhow!("Missing required parameter: {}", key))
    }

    /// Gets a required boolean parameter from the URL.
    ///
    /// Returns `Ok(value)` if the parameter exists and is a valid boolean,
    /// `Err` if the parameter is missing or cannot be parsed as a boolean.
    pub fn get_required_bool(&self, key: &str) -> Result<bool> {
        self.get_bool(key)?
            .ok_or_else(|| anyhow!("Missing required parameter: {}", key))
    }

    /// Gets a required integer parameter from the URL.
    ///
    /// Returns `Ok(value)` if the parameter exists and is a valid integer,
    /// `Err` if the parameter is missing or cannot be parsed as an integer.
    pub fn get_required_int(&self, key: &str) -> Result<i64> {
        self.get_int(key)?
            .ok_or_else(|| anyhow!("Missing required parameter: {}", key))
    }

    /// Gets a required floating-point parameter from the URL.
    ///
    /// Returns `Ok(value)` if the parameter exists and is a valid float,
    /// `Err` if the parameter is missing or cannot be parsed as a float.
    pub fn get_required_float(&self, key: &str) -> Result<f64> {
        self.get_float(key)?
            .ok_or_else(|| anyhow!("Missing required parameter: {}", key))
    }

    /// Finalizes the URL, preventing any further mutations.
    ///
    /// After calling this method, add_* methods will panic if called.
    /// The URL can only be extracted via `url()` after finalization.
    pub fn finalize(&mut self) {
        self.finalized = true;
    }

    /// Returns the final URL string with the specified protocol.
    ///
    /// # Arguments
    /// * `secure` - If true, uses "https://", otherwise uses "http://"
    ///
    /// # Errors
    /// Returns an error if the URL has not been finalized.
    pub fn url(&self, secure: bool) -> Result<String> {
        if !self.finalized {
            bail!("DataUrl must be finalized before calling url()");
        }

        let protocol = if secure { "https" } else { "http" };
        let base = format!("{}://{}/{}", protocol, self.domain, self.path);

        let mut url = Url::parse(&base)?;
        url.set_query(self.url.query());

        Ok(url.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_basic() {
        let url = DataUrl::new("example.com", "beacon").unwrap();
        assert_eq!(url.domain, "example.com");
        assert_eq!(url.path, "beacon");
        assert!(!url.finalized);
    }

    #[test]
    fn test_new_handles_slashes() {
        let url1 = DataUrl::new("example.com/", "/beacon").unwrap();
        assert_eq!(url1.domain, "example.com");
        assert_eq!(url1.path, "beacon");

        let url2 = DataUrl::new("example.com", "beacon/").unwrap();
        assert_eq!(url2.domain, "example.com");
        assert_eq!(url2.path, "beacon/");

        let url3 = DataUrl::new("example.com", "/beacon/").unwrap();
        assert_eq!(url3.domain, "example.com");
        assert_eq!(url3.path, "beacon/");

        // All should produce valid URLs
        url1.clone().finalize();
        url2.clone().finalize();
        url3.clone().finalize();
    }

    #[test]
    fn test_add_and_get_string() {
        let mut url = DataUrl::new("example.com", "beacon").unwrap();
        url.add_string("auction_id", "abc123").unwrap();

        let value = url.get_string("auction_id").unwrap();
        assert_eq!(value, Some("abc123".to_string()));

        let missing = url.get_string("missing").unwrap();
        assert_eq!(missing, None);
    }

    #[test]
    fn test_add_and_get_bool() {
        let mut url = DataUrl::new("example.com", "beacon").unwrap();
        url.add_bool("won", true).unwrap();
        url.add_bool("lost", false).unwrap();

        assert_eq!(url.get_bool("won").unwrap(), Some(true));
        assert_eq!(url.get_bool("lost").unwrap(), Some(false));
        assert_eq!(url.get_bool("missing").unwrap(), None);
    }

    #[test]
    fn test_add_and_get_int() {
        let mut url = DataUrl::new("example.com", "beacon").unwrap();
        url.add_int("price", 150).unwrap();
        url.add_int("negative", -42).unwrap();

        assert_eq!(url.get_int("price").unwrap(), Some(150));
        assert_eq!(url.get_int("negative").unwrap(), Some(-42));
        assert_eq!(url.get_int("missing").unwrap(), None);
    }

    #[test]
    fn test_add_and_get_float() {
        let mut url = DataUrl::new("example.com", "beacon").unwrap();
        url.add_float("price", 1.50).unwrap();

        assert_eq!(url.get_float("price").unwrap(), Some(1.50));
        assert_eq!(url.get_float("missing").unwrap(), None);
    }

    #[test]
    fn test_finalize_and_url() {
        let mut url = DataUrl::new("example.com", "beacon").unwrap();
        url.add_string("id", "123")
            .unwrap()
            .add_bool("won", true)
            .unwrap();

        // Cannot get URL before finalization
        assert!(url.url(true).is_err());

        url.finalize();

        // Can get URL after finalization
        let https_url = url.url(true).unwrap();
        assert!(https_url.starts_with("https://"));
        assert!(https_url.contains("id=123"));
        assert!(https_url.contains("won=true"));

        let http_url = url.url(false).unwrap();
        assert!(http_url.starts_with("http://"));
    }

    #[test]
    fn test_cannot_add_after_finalize() {
        let mut url = DataUrl::new("example.com", "beacon").unwrap();
        url.finalize();
        let result = url.add_string("key", "value");
        assert!(result.is_err());
    }

    #[test]
    fn test_from_parses_url() {
        let url_str = "https://example.com/beacon?id=123&won=true&price=150";
        let url = DataUrl::from(url_str).unwrap();

        assert_eq!(url.domain, "example.com");
        assert_eq!(url.path, "beacon");
        assert!(url.finalized);

        assert_eq!(url.get_string("id").unwrap(), Some("123".to_string()));
        assert_eq!(url.get_bool("won").unwrap(), Some(true));
        assert_eq!(url.get_int("price").unwrap(), Some(150));
    }

    #[test]
    fn test_from_is_finalized() {
        let url = DataUrl::from("https://example.com/beacon").unwrap();
        assert!(url.finalized);

        // Should be able to get URL immediately
        let result = url.url(true).unwrap();
        assert!(result.starts_with("https://"));
    }

    #[test]
    fn test_chainable_builder() {
        let mut url = DataUrl::new("example.com", "beacon").unwrap();
        url.add_string("id", "abc")
            .unwrap()
            .add_bool("won", true)
            .unwrap()
            .add_int("price", 100)
            .unwrap()
            .add_float("margin", 0.25)
            .unwrap();

        assert_eq!(url.get_string("id").unwrap(), Some("abc".to_string()));
        assert_eq!(url.get_bool("won").unwrap(), Some(true));
        assert_eq!(url.get_int("price").unwrap(), Some(100));
        assert_eq!(url.get_float("margin").unwrap(), Some(0.25));
    }

    #[test]
    fn test_clone_and_debug() {
        let mut url = DataUrl::new("example.com", "beacon").unwrap();
        url.add_string("id", "123").unwrap();

        let cloned = url.clone();
        assert_eq!(cloned.get_string("id").unwrap(), Some("123".to_string()));

        // Debug should work
        let debug_str = format!("{:?}", url);
        assert!(debug_str.contains("DataUrl"));
    }

    #[test]
    fn test_get_parse_error() {
        let mut url = DataUrl::new("example.com", "beacon").unwrap();
        url.add_string("not_a_bool", "invalid").unwrap();

        // Should return error when trying to parse as bool
        let result = url.get_bool("not_a_bool");
        assert!(result.is_err());
    }

    #[test]
    fn test_url_encoding() {
        let mut url = DataUrl::new("example.com", "beacon").unwrap();
        url.add_string("special", "hello world").unwrap();
        url.finalize();

        let final_url = url.url(true).unwrap();
        // Space should be encoded (URL encoding can use + or %20)
        assert!(final_url.contains("hello+world") || final_url.contains("hello%20world"));
    }

    #[test]
    fn test_get_required_string() {
        let mut url = DataUrl::new("example.com", "beacon").unwrap();
        url.add_string("name", "test").unwrap();

        // Should return value when present
        let result = url.get_required_string("name").unwrap();
        assert_eq!(result, "test");

        // Should error when missing
        let result = url.get_required_string("missing");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Missing required parameter")
        );
    }

    #[test]
    fn test_get_required_bool() {
        let mut url = DataUrl::new("example.com", "beacon").unwrap();
        url.add_bool("flag", true).unwrap();

        // Should return value when present
        let result = url.get_required_bool("flag").unwrap();
        assert_eq!(result, true);

        // Should error when missing
        let result = url.get_required_bool("missing");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Missing required parameter")
        );

        // Should error when invalid
        url.add_string("invalid", "not_a_bool").unwrap();
        let result = url.get_required_bool("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_required_int() {
        let mut url = DataUrl::new("example.com", "beacon").unwrap();
        url.add_int("count", 42).unwrap();

        // Should return value when present
        let result = url.get_required_int("count").unwrap();
        assert_eq!(result, 42);

        // Should error when missing
        let result = url.get_required_int("missing");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Missing required parameter")
        );

        // Should error when invalid
        url.add_string("invalid", "not_an_int").unwrap();
        let result = url.get_required_int("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_required_float() {
        let mut url = DataUrl::new("example.com", "beacon").unwrap();
        url.add_float("price", 12.99).unwrap();

        // Should return value when present
        let result = url.get_required_float("price").unwrap();
        assert_eq!(result, 12.99);

        // Should error when missing
        let result = url.get_required_float("missing");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Missing required parameter")
        );

        // Should error when invalid
        url.add_string("invalid", "not_a_float").unwrap();
        let result = url.get_required_float("invalid");
        assert!(result.is_err());
    }
}
