#!/usr/bin/env python3
"""
Validate GitHub Actions workflow files locally without committing.

This script checks:
1. YAML syntax validity
2. Basic workflow structure
3. Common GitHub Actions workflow errors
"""

import os
import sys
import yaml
from pathlib import Path
from typing import List, Tuple

# Color codes for terminal output
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"


def validate_yaml_syntax(file_path: Path) -> Tuple[bool, str]:
    """Validate YAML syntax of a workflow file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            yaml.safe_load(f)
        return True, ""
    except yaml.YAMLError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Error reading file: {e}"


def validate_workflow_structure(workflow_data: dict, file_path: Path) -> List[str]:
    """Validate basic workflow structure and common issues."""
    errors = []
    
    # Check required top-level keys
    # Note: YAML parses 'on' as True (boolean), so we check for both
    has_on = "on" in workflow_data or True in workflow_data
    
    if "name" not in workflow_data:
        errors.append("Missing required key: 'name'")
    
    if not has_on:
        errors.append("Missing required key: 'on'")
    
    if "jobs" not in workflow_data:
        errors.append("Missing required key: 'jobs'")
    
    # Validate 'on' trigger (check both 'on' and True since YAML converts it)
    on_value = workflow_data.get("on") or workflow_data.get(True)
    if on_value is not None:
        if not isinstance(on_value, (dict, list, str)):
            errors.append("'on' must be a string, list, or dictionary")
    
    # Validate jobs
    if "jobs" in workflow_data:
        if not isinstance(workflow_data["jobs"], dict):
            errors.append("'jobs' must be a dictionary")
        else:
            for job_name, job_config in workflow_data["jobs"].items():
                if not isinstance(job_config, dict):
                    errors.append(f"Job '{job_name}' must be a dictionary")
                    continue
                
                # Check for invalid service keys
                if "services" in job_config:
                    services = job_config["services"]
                    if isinstance(services, dict):
                        for service_name, service_config in services.items():
                            if isinstance(service_config, dict):
                                # Check for invalid 'environment' key (should be 'env')
                                if "environment" in service_config:
                                    errors.append(
                                        f"Job '{job_name}' -> Service '{service_name}': "
                                        f"Use 'env' instead of 'environment' for service environment variables"
                                    )
                                # Check for invalid 'command' key
                                if "command" in service_config:
                                    errors.append(
                                        f"Job '{job_name}' -> Service '{service_name}': "
                                        f"'command' is not a valid key for services. Use 'options' or remove it."
                                    )
    
    return errors


def validate_workflow_file(file_path: Path) -> Tuple[bool, List[str]]:
    """Validate a single workflow file."""
    errors = []
    
    # Load and validate structure
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            workflow_data = yaml.safe_load(f)
        
        if workflow_data is None:
            errors.append("Workflow file is empty or invalid YAML")
            return False, errors
        
        # Check YAML syntax (already loaded, but verify it's valid)
        try:
            yaml.safe_load(open(file_path, "r", encoding="utf-8"))
        except yaml.YAMLError as e:
            errors.append(f"YAML syntax error: {e}")
            return False, errors
        
        # Debug: Print loaded keys
        if not workflow_data:
            errors.append("Workflow file is empty or could not be parsed")
            return False, errors
        
        # Debug output (can be removed later)
        # print(f"DEBUG: Loaded keys: {list(workflow_data.keys())}")
        
        structure_errors = validate_workflow_structure(workflow_data, file_path)
        errors.extend(structure_errors)
        
        return len(errors) == 0, errors
    except yaml.YAMLError as e:
        errors.append(f"YAML syntax error: {e}")
        return False, errors
    except Exception as e:
        errors.append(f"Error validating structure: {e}")
        return False, errors


def main():
    """Main validation function."""
    script_dir = Path(__file__).parent.absolute()
    workflows_dir = script_dir / ".github" / "workflows"
    
    if not workflows_dir.exists():
        print(f"{RED}✗ Workflows directory not found: {workflows_dir}{RESET}")
        sys.exit(1)
    
    workflow_files = list(workflows_dir.glob("*.yml")) + list(workflows_dir.glob("*.yaml"))
    
    if not workflow_files:
        print(f"{YELLOW}⚠ No workflow files found in {workflows_dir}{RESET}")
        sys.exit(0)
    
    print(f"{BLUE}Validating GitHub Actions workflows...{RESET}\n")
    
    all_valid = True
    for workflow_file in sorted(workflow_files):
        print(f"{BLUE}Checking: {workflow_file.name}{RESET}")
        is_valid, errors = validate_workflow_file(workflow_file)
        
        if is_valid:
            print(f"  {GREEN}✓ Valid{RESET}\n")
        else:
            all_valid = False
            print(f"  {RED}✗ Invalid{RESET}")
            for error in errors:
                print(f"    {RED}  - {error}{RESET}")
            print()
    
    if all_valid:
        print(f"{GREEN}✓ All workflows are valid!{RESET}")
        sys.exit(0)
    else:
        print(f"{RED}✗ Some workflows have errors. Please fix them before committing.{RESET}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        import yaml
    except ImportError:
        print(f"{RED}✗ PyYAML is required. Install it with: pip install pyyaml{RESET}")
        sys.exit(1)
    
    main()

