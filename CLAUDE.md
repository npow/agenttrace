
## Claude Retro Suggestions
<!-- claude-retro-auto -->
- Before implementing features requiring data queries or API calls, always run a quick query/API call first to confirm the data structure matches what the UI/output will display, even if it seems obvious. Don't discover mid-implementation that the API returns different field names or shapes.
- When the user says 'DO EVERYTHING', 'just finish it', 'continue', or 'work autonomously', do not pause to ask clarifying questions unless truly blocked. Instead, proceed with best judgment and include a checkpoint sentence like 'I'm proceeding autonomously; I'll run [VERIFICATION STEP] and circle back if blocked.' Re-read the conversation for tone signals before asking.
- When investigating backend failures (data not appearing, API returning wrong shapes, queries failing), start by directly querying the data source (run a test query, check database rows, inspect API response) before investigating application code. A 2-minute data check often reveals the root cause faster than 20 turns of code review.
- After implementing any feature that involves LLM calls, database writes, or API integrations, run a complete end-to-end test with real data (not code inspection): verify rows appear in the actual database table, API responses contain expected fields, and the UI displays the data. Do not rely on code review or mock tests to confirm data flow.
- When setting up integrations with external tools (MCP servers, Jira APIs, credentials, GitHub actions), immediately ask the user for all required configuration details (cloud IDs, API keys, service URLs) before attempting any tool calls. A 2-minute credential clarification beats 60 turns of failed attempts.
<!-- claude-retro-auto -->
