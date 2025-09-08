"""
Security utilities and IAM management for the video analytics pipeline.
"""

import json
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path

from google.cloud import iam
from google.oauth2 import service_account
from google.auth import default


class IAMManager:
    """Manage IAM roles and permissions for the pipeline."""
    
    def __init__(self, project_id: str):
        """
        Initialize IAM manager.
        
        Args:
            project_id: Google Cloud project ID
        """
        self.project_id = project_id
        self.iam_client = iam.IAMCredentialsServiceClient()
    
    def create_service_account_config(self, service_account_name: str) -> Dict[str, Any]:
        """
        Create service account configuration for the pipeline.
        
        Args:
            service_account_name: Name of the service account
            
        Returns:
            Dictionary containing service account configuration
        """
        required_roles = [
            "roles/dataflow.worker",
            "roles/dataflow.developer", 
            "roles/pubsub.subscriber",
            "roles/pubsub.publisher",
            "roles/storage.objectAdmin",
            "roles/monitoring.metricWriter",
            "roles/logging.logWriter",
            "roles/cloudtrace.agent"
        ]
        
        service_account_email = f"{service_account_name}@{self.project_id}.iam.gserviceaccount.com"
        
        config = {
            "service_account": {
                "name": service_account_name,
                "email": service_account_email,
                "display_name": "Video Analytics Pipeline Service Account",
                "description": "Service account for the video analytics streaming pipeline"
            },
            "required_roles": required_roles,
            "resource_bindings": {
                f"projects/{self.project_id}": required_roles,
                f"projects/{self.project_id}/topics/*": [
                    "roles/pubsub.publisher",
                    "roles/pubsub.subscriber"
                ],
                f"projects/{self.project_id}/subscriptions/*": [
                    "roles/pubsub.subscriber"
                ]
            }
        }
        
        return config
    
    def get_iam_policy_template(self) -> Dict[str, Any]:
        """
        Get IAM policy template for the pipeline resources.
        
        Returns:
            Dictionary containing IAM policy template
        """
        return {
            "version": 1,
            "bindings": [
                {
                    "role": "roles/pubsub.admin",
                    "members": [
                        "serviceAccount:video-analytics-pipeline@{project_id}.iam.gserviceaccount.com"
                    ],
                    "condition": None
                },
                {
                    "role": "roles/dataflow.admin", 
                    "members": [
                        "serviceAccount:video-analytics-pipeline@{project_id}.iam.gserviceaccount.com"
                    ],
                    "condition": None
                },
                {
                    "role": "roles/storage.admin",
                    "members": [
                        "serviceAccount:video-analytics-pipeline@{project_id}.iam.gserviceaccount.com"
                    ],
                    "condition": {
                        "title": "Pipeline Storage Access",
                        "description": "Access to pipeline staging and temp buckets",
                        "expression": "resource.name.startsWith('projects/_/buckets/video-analytics-')"
                    }
                }
            ]
        }


class SecurityValidator:
    """Validate security configurations and compliance."""
    
    def __init__(self):
        """Initialize security validator."""
        self.security_checks = {
            "encryption_at_rest": self._check_encryption_at_rest,
            "encryption_in_transit": self._check_encryption_in_transit,
            "service_account_permissions": self._check_service_account_permissions,
            "network_security": self._check_network_security,
            "data_access_logging": self._check_data_access_logging
        }
    
    def validate_security_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate security configuration against best practices.
        
        Args:
            config: Security configuration to validate
            
        Returns:
            Dictionary containing validation results
        """
        results = {
            "overall_status": "compliant",
            "checks": {},
            "violations": [],
            "warnings": [],
            "recommendations": []
        }
        
        for check_name, check_func in self.security_checks.items():
            try:
                check_result = check_func(config)
                results["checks"][check_name] = check_result
                
                if not check_result["passed"]:
                    results["violations"].append({
                        "check": check_name,
                        "message": check_result["message"],
                        "severity": check_result.get("severity", "medium")
                    })
                    
                    if check_result.get("severity") == "high":
                        results["overall_status"] = "non_compliant"
                
                if check_result.get("warnings"):
                    results["warnings"].extend(check_result["warnings"])
                
                if check_result.get("recommendations"):
                    results["recommendations"].extend(check_result["recommendations"])
                    
            except Exception as e:
                logging.error(f"Security check {check_name} failed: {e}")
                results["checks"][check_name] = {
                    "passed": False,
                    "message": f"Check failed: {str(e)}",
                    "severity": "high"
                }
        
        return results
    
    def _check_encryption_at_rest(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Check encryption at rest configuration."""
        result = {
            "passed": True,
            "message": "Encryption at rest is properly configured",
            "warnings": [],
            "recommendations": []
        }
        
        # Check Pub/Sub encryption
        pubsub_config = config.get("pubsub", {})
        if not pubsub_config.get("encryption_key"):
            result["warnings"].append("Pub/Sub not using customer-managed encryption keys")
            result["recommendations"].append("Consider using CMEK for Pub/Sub topics")
        
        # Check Cloud Storage encryption
        storage_config = config.get("storage", {})
        if not storage_config.get("encryption_key"):
            result["warnings"].append("Cloud Storage not using customer-managed encryption keys")
            result["recommendations"].append("Consider using CMEK for Cloud Storage buckets")
        
        return result
    
    def _check_encryption_in_transit(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Check encryption in transit configuration."""
        result = {
            "passed": True,
            "message": "Encryption in transit is enforced",
            "warnings": [],
            "recommendations": []
        }
        
        # GCP services use TLS by default, but check configuration
        network_config = config.get("network", {})
        if network_config.get("allow_http", False):
            result["passed"] = False
            result["message"] = "HTTP traffic is allowed, violates encryption in transit"
            result["severity"] = "high"
        
        return result
    
    def _check_service_account_permissions(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Check service account permissions follow principle of least privilege."""
        result = {
            "passed": True,
            "message": "Service account permissions are appropriate",
            "warnings": [],
            "recommendations": []
        }
        
        iam_config = config.get("iam", {})
        service_accounts = iam_config.get("service_accounts", [])
        
        for sa in service_accounts:
            roles = sa.get("roles", [])
            
            # Check for overly broad roles
            broad_roles = ["roles/owner", "roles/editor", "roles/iam.securityAdmin"]
            for role in roles:
                if role in broad_roles:
                    result["passed"] = False
                    result["message"] = f"Service account has overly broad role: {role}"
                    result["severity"] = "high"
                    break
            
            # Check for admin roles
            admin_roles = [role for role in roles if "admin" in role.lower()]
            if admin_roles:
                result["warnings"].append(f"Service account has admin roles: {admin_roles}")
                result["recommendations"].append("Review if admin roles are necessary")
        
        return result
    
    def _check_network_security(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Check network security configuration."""
        result = {
            "passed": True,
            "message": "Network security is properly configured",
            "warnings": [],
            "recommendations": []
        }
        
        network_config = config.get("network", {})
        
        # Check if using private networks
        if not network_config.get("use_private_ip", False):
            result["warnings"].append("Not using private IP addresses")
            result["recommendations"].append("Consider using private networks for enhanced security")
        
        # Check firewall rules
        if not network_config.get("firewall_rules"):
            result["warnings"].append("No custom firewall rules defined")
            result["recommendations"].append("Define specific firewall rules for the pipeline")
        
        return result
    
    def _check_data_access_logging(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Check data access logging configuration."""
        result = {
            "passed": True,
            "message": "Data access logging is enabled",
            "warnings": [],
            "recommendations": []
        }
        
        logging_config = config.get("logging", {})
        
        # Check audit logging
        if not logging_config.get("audit_logs_enabled", False):
            result["passed"] = False
            result["message"] = "Audit logging is not enabled"
            result["severity"] = "medium"
        
        # Check data access logs
        if not logging_config.get("data_access_logs", False):
            result["warnings"].append("Data access logs are not enabled")
            result["recommendations"].append("Enable data access logs for compliance")
        
        return result


class SecretsManager:
    """Manage secrets and sensitive configuration."""
    
    def __init__(self, project_id: str):
        """
        Initialize secrets manager.
        
        Args:
            project_id: Google Cloud project ID
        """
        self.project_id = project_id
    
    def create_secret_config_template(self) -> Dict[str, Any]:
        """
        Create template for secrets configuration.
        
        Returns:
            Dictionary containing secrets template
        """
        return {
            "secrets": {
                "api_keys": {
                    "secret_name": "video-analytics-api-keys",
                    "description": "API keys for external services",
                    "labels": {
                        "component": "video-analytics-pipeline",
                        "environment": "production"
                    }
                },
                "database_credentials": {
                    "secret_name": "video-analytics-db-creds",
                    "description": "Database connection credentials",
                    "labels": {
                        "component": "video-analytics-pipeline",
                        "environment": "production"
                    }
                },
                "encryption_keys": {
                    "secret_name": "video-analytics-encryption-keys",
                    "description": "Encryption keys for data processing",
                    "labels": {
                        "component": "video-analytics-pipeline",
                        "environment": "production"
                    }
                }
            },
            "access_policies": {
                "api_keys": [
                    "serviceAccount:video-analytics-pipeline@{project_id}.iam.gserviceaccount.com"
                ],
                "database_credentials": [
                    "serviceAccount:video-analytics-pipeline@{project_id}.iam.gserviceaccount.com"
                ],
                "encryption_keys": [
                    "serviceAccount:video-analytics-pipeline@{project_id}.iam.gserviceaccount.com"
                ]
            }
        }
    
    def validate_secret_access(self, secret_name: str, service_account: str) -> bool:
        """
        Validate that a service account has access to a secret.
        
        Args:
            secret_name: Name of the secret
            service_account: Service account email
            
        Returns:
            True if access is configured, False otherwise
        """
        # In a real implementation, this would check Secret Manager IAM policies
        logging.info(f"Validating access to secret {secret_name} for {service_account}")
        return True


def create_security_deployment_guide() -> Dict[str, Any]:
    """
    Create a comprehensive security deployment guide.
    
    Returns:
        Dictionary containing security deployment instructions
    """
    return {
        "pre_deployment": {
            "title": "Pre-deployment Security Checklist",
            "steps": [
                "Review and approve IAM roles and permissions",
                "Validate encryption configuration for all components",
                "Ensure audit logging is enabled",
                "Configure network security groups and firewall rules",
                "Set up monitoring and alerting for security events",
                "Review and approve data retention policies"
            ]
        },
        "deployment": {
            "title": "Secure Deployment Process",
            "steps": [
                "Deploy using service accounts with minimal permissions",
                "Enable private networks and disable public IP addresses",
                "Configure customer-managed encryption keys (CMEK)",
                "Set up VPC firewall rules",
                "Enable audit logging and monitoring",
                "Validate security configurations before going live"
            ]
        },
        "post_deployment": {
            "title": "Post-deployment Security Validation",
            "steps": [
                "Verify all components are using encrypted connections",
                "Confirm audit logs are being generated",
                "Test access controls and permissions",
                "Review monitoring alerts and dashboards",
                "Conduct security scanning and vulnerability assessment",
                "Document security configuration and procedures"
            ]
        },
        "ongoing_maintenance": {
            "title": "Ongoing Security Maintenance",
            "steps": [
                "Regular security configuration reviews",
                "Monitor and respond to security alerts",
                "Update service account permissions as needed",
                "Rotate encryption keys and secrets regularly",
                "Review and update access controls",
                "Conduct periodic security assessments"
            ]
        }
    }