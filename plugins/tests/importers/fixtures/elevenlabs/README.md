# ElevenLabs conversation fixtures

These are sanitized `GET /v1/convai/conversations/{conversation_id}` responses from five real voice calls to a temporary Kitaru test-shop agent on October 2, 2026. The calls used the ElevenLabs JavaScript SDK 1.26.0, client tools, and the `qwen35-397b-a17b` model. The shop, orders, return policy, and deliberate tool failure are synthetic lab scenarios, not customer conversations.

| Fixture | Recorded behavior |
| --- | --- |
| `happy_path.json` | A successful `get_order` call followed by an agent answer. |
| `multiple_tools.json` | `get_order` followed by `get_return_policy` within one conversation. |
| `tool_error.json` | A deliberate client-tool execution error, followed by a successful conversational recovery. |
| `interruption.json` | An interrupted agent response preserves both spoken text and `original_message`. |
| `not_found.json` | Two separate `get_order` requests return `found: false` without a tool execution error. |

Agent, branch, version, conversation, voice, and tool request IDs were replaced consistently with synthetic fixture IDs. The lab agent name was replaced. Transcript text, tool arguments/results, timings, latency metrics, token counts, reported prices, charging breakdowns, analysis, and audio-availability flags retain the API response shape and values. No API credentials, original account identifiers, or audio files are included.

The original JSON and MP3 exports remain in the ignored local `design/elevenlabs-lab/exports/` directory. These JSON fixtures are sufficient to test parsing without an ElevenLabs account or network access. They do not verify ElevenLabs retention settings, webhook delivery, audio playback, or conversation replay.

API reference: [Get conversation details](https://elevenlabs.io/docs/api-reference/conversations/get).
