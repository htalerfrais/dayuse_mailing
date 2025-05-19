-- Drop the HuggingFace external access integration and the network rule it relies on.
DROP NETWORK RULE hf_network_rule;
DROP EXTERNAL ACCESS INTEGRATION hf_access_integration;

-- Drop the Github external access integration and the network rule it relies on.
DROP NETWORK RULE gh_network_rule;
DROP EXTERNAL ACCESS INTEGRATION gh_access_integration;
