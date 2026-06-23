from demo import write_comparison_html
from dotenv import load_dotenv
from support_agent import CUT, support_copilot_flow
from utils import load_support_decision_from_execution

from kitaru import KitaruClient

load_dotenv()

client = KitaruClient()

SCENARIO = (
    "I need to grant all members of our engineering team admin access to the "
    "production SSO settings so they can self-service identity provider changes "
    "without going through IT. Can you enable that for our account?"
)
CUSTOMER = "acme-corp / alice@acme.example"


# 1) Original production run — gpt-5-mini, baseline prompts.
handle = support_copilot_flow.run(SCENARIO, CUSTOMER, "openai:gpt-5-mini", "baseline")
handle.wait()
original_id = handle.exec_id

# 2) Reproduce from `decide`, NO edits.
repro = support_copilot_flow.replay(original_id, from_=CUT, cache=False)
repro.wait()
repro_id = repro.exec_id

# 3) Edited replay from `decide` — cheaper model (gpt-5-nano) + looser prompt.
edited = support_copilot_flow.replay(
    original_id,
    from_=CUT,
    cache=False,
    model="openai:gpt-5-nano",
    prompt_profile="trimmed_permissions",
)
edited.wait()
edited_id = edited.exec_id

# 4) Read the decisions back from each execution.
original_decision = load_support_decision_from_execution(client, original_id)
repro_decision = load_support_decision_from_execution(client, repro_id)
edited_decision = load_support_decision_from_execution(client, edited_id)

print(f"Original Decision: {original_decision}")
print(f"Reproduced Decision: {repro_decision}")
print(f"Edited Decision: {edited_decision}")

# 5) Write the three-way original/reproduced/edited comparison HTML.
path = write_comparison_html(
    original_id, original_decision, repro_decision, edited_decision
)
print(f"HTML: {path}")
