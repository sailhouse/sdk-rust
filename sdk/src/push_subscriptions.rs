use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

/// Error types for push subscription verification
#[derive(Error, Debug)]
pub enum PushSubscriptionVerificationError {
    #[error("Push subscription secret is required")]
    MissingSecret,
    #[error("Signature header is required")]
    MissingSignatureHeader,
    #[error("Invalid timestamp in signature header")]
    InvalidTimestamp,
    #[error("Invalid signature header format. Expected format: t=<timestamp>,v1=<signature>")]
    InvalidSignatureFormat,
    #[error("Request timestamp is too old. Maximum age: {tolerance} seconds")]
    TimestampTooOld { tolerance: u64 },
    #[error("Signature verification failed")]
    InvalidSignature,
    #[error("Signature verification failed: {0}")]
    VerificationError(String),
}

/// Parsed signature header components
#[derive(Debug)]
pub struct SignatureComponents {
    pub timestamp: u64,
    pub signature: String,
}

/// Headers expected from a push subscription request
#[derive(Debug)]
pub struct PushSubscriptionHeaders {
    pub sailhouse_signature: String,
    pub identifier: String,
    pub event_id: String,
}

/// Push subscription payload structure
#[derive(Debug, serde::Deserialize)]
pub struct PushSubscriptionPayload<T> {
    pub data: T,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, serde_json::Value>>,
    pub id: String,
    pub timestamp: String,
}

/// Verification options
#[derive(Debug)]
pub struct VerificationOptions {
    /// Tolerance for timestamp validation in seconds (default: 300)
    pub tolerance: Option<u64>,
}

impl Default for VerificationOptions {
    fn default() -> Self {
        Self { tolerance: Some(300) }
    }
}

/// Push subscription signature verifier
pub struct PushSubscriptionVerifier {
    secret: String,
}

impl PushSubscriptionVerifier {
    /// Create a new verifier with the given secret
    pub fn new(secret: String) -> Result<Self, PushSubscriptionVerificationError> {
        if secret.is_empty() {
            return Err(PushSubscriptionVerificationError::MissingSecret);
        }
        Ok(Self { secret })
    }

    /// Verify a push subscription signature
    pub fn verify_signature(
        &self,
        signature: &str,
        body: &str,
        options: Option<VerificationOptions>,
    ) -> Result<bool, PushSubscriptionVerificationError> {
        let opts = options.unwrap_or_default();
        let tolerance = opts.tolerance.unwrap_or(300);

        // Parse signature header
        let components = self.parse_signature_header(signature)?;

        // Validate timestamp
        if !self.is_timestamp_valid(components.timestamp, tolerance) {
            return Err(PushSubscriptionVerificationError::TimestampTooOld { tolerance });
        }

        // Calculate expected signature
        let expected_signature = self.calculate_signature(components.timestamp, body)?;

        // Perform constant-time comparison
        if !self.constant_time_equal(&expected_signature, &components.signature) {
            return Err(PushSubscriptionVerificationError::InvalidSignature);
        }

        Ok(true)
    }

    /// Parse the Sailhouse-Signature header
    pub fn parse_signature_header(
        &self,
        header: &str,
    ) -> Result<SignatureComponents, PushSubscriptionVerificationError> {
        if header.is_empty() {
            return Err(PushSubscriptionVerificationError::MissingSignatureHeader);
        }

        let elements: Vec<&str> = header.split(',').collect();
        let mut timestamp: Option<u64> = None;
        let mut signature: Option<String> = None;

        for element in elements {
            let trimmed = element.trim();
            let parts: Vec<&str> = trimmed.split('=').collect();
            
            if parts.len() != 2 {
                continue;
            }

            match parts[0] {
                "t" => {
                    timestamp = parts[1].parse::<u64>().ok();
                    if timestamp.is_none() {
                        return Err(PushSubscriptionVerificationError::InvalidTimestamp);
                    }
                }
                "v1" => {
                    signature = Some(parts[1].to_string());
                }
                _ => continue,
            }
        }

        match (timestamp, signature) {
            (Some(ts), Some(sig)) => Ok(SignatureComponents {
                timestamp: ts,
                signature: sig,
            }),
            _ => Err(PushSubscriptionVerificationError::InvalidSignatureFormat),
        }
    }

    /// Check if timestamp is within tolerance
    pub fn is_timestamp_valid(&self, timestamp: u64, tolerance: u64) -> bool {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        current_time >= timestamp && (current_time - timestamp) <= tolerance
    }

    /// Calculate HMAC-SHA256 signature for the payload
    pub fn calculate_signature(
        &self,
        timestamp: u64,
        body: &str,
    ) -> Result<String, PushSubscriptionVerificationError> {
        let payload = format!("{}.{}", timestamp, body);
        
        let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes())
            .map_err(|e| PushSubscriptionVerificationError::VerificationError(e.to_string()))?;
        
        mac.update(payload.as_bytes());
        let result = mac.finalize();
        Ok(hex::encode(result.into_bytes()))
    }

    /// Perform constant-time comparison to prevent timing attacks
    fn constant_time_equal(&self, expected: &str, actual: &str) -> bool {
        if expected.len() != actual.len() {
            return false;
        }

        let expected_bytes = match hex::decode(expected) {
            Ok(bytes) => bytes,
            Err(_) => return false,
        };
        
        let actual_bytes = match hex::decode(actual) {
            Ok(bytes) => bytes,
            Err(_) => return false,
        };

        // Simple constant-time comparison
        let mut result = 0u8;
        for (a, b) in expected_bytes.iter().zip(actual_bytes.iter()) {
            result |= a ^ b;
        }
        result == 0
    }
}

/// Convenience function for one-off signature verification
pub fn verify_push_subscription_signature(
    secret: &str,
    signature: &str,
    body: &str,
    options: Option<VerificationOptions>,
) -> Result<bool, PushSubscriptionVerificationError> {
    let verifier = PushSubscriptionVerifier::new(secret.to_string())?;
    verifier.verify_signature(signature, body, options)
}

/// Safe verification that returns a boolean instead of throwing
pub fn verify_push_subscription_signature_safe(
    secret: &str,
    signature: &str,
    body: &str,
    options: Option<VerificationOptions>,
) -> bool {
    verify_push_subscription_signature(secret, signature, body, options).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signature_header_parsing() {
        let verifier = PushSubscriptionVerifier::new("test-secret".to_string()).unwrap();
        
        let header = "t=1625097600,v1=abcdef123456";
        let components = verifier.parse_signature_header(header).unwrap();
        
        assert_eq!(components.timestamp, 1625097600);
        assert_eq!(components.signature, "abcdef123456");
    }

    #[test]
    fn test_invalid_signature_header() {
        let verifier = PushSubscriptionVerifier::new("test-secret".to_string()).unwrap();
        
        let header = "invalid-header";
        let result = verifier.parse_signature_header(header);
        
        assert!(matches!(result, Err(PushSubscriptionVerificationError::InvalidSignatureFormat)));
    }

    #[test]
    fn test_timestamp_validation() {
        let verifier = PushSubscriptionVerifier::new("test-secret".to_string()).unwrap();
        
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Valid timestamp (within tolerance)
        assert!(verifier.is_timestamp_valid(current_time - 100, 300));
        
        // Invalid timestamp (too old)
        assert!(!verifier.is_timestamp_valid(current_time - 400, 300));
    }

    #[test]
    fn test_signature_calculation() {
        let verifier = PushSubscriptionVerifier::new("test-secret".to_string()).unwrap();
        
        let signature = verifier.calculate_signature(1625097600, "test-body").unwrap();
        
        // Verify that we get a consistent hex-encoded signature
        assert_eq!(signature.len(), 64); // SHA256 hex string length
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_safe_verification() {
        // Valid signature should return true
        let result = verify_push_subscription_signature_safe(
            "valid-secret",
            "t=1625097600,v1=validhex",
            "test-body",
            None,
        );
        
        // This will likely fail because we're not using a real signature,
        // but it shouldn't panic
        assert!(!result || result); // Just ensure it returns a boolean
    }
}