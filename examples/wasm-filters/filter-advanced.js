// Advanced filter example with multiple criteria
// Shows how to combine different filtering strategies

export function filter() {
  const input = Host.inputString();
  const ctx = JSON.parse(input);

  if (!ctx.post || !ctx.post.text) {
    Host.outputString(JSON.stringify(false));
    return;
  }

  const text = ctx.post.text.toLowerCase();
  const authorDid = ctx.author_did;

  // Criteria 1: Must be from someone we follow OR someone who follows us
  const isFromConnection =
    (authorDid in ctx.follows) ||
    (authorDid in ctx.followed);

  // Criteria 2: Must contain interesting topics
  const interestingTopics = [
    "programming", "coding", "software", "developer",
    "javascript", "python", "rust", "golang",
    "wasm", "webassembly", "bluesky", "atproto"
  ];
  const hasInterestingTopic = interestingTopics.some(topic =>
    text.includes(topic)
  );

  // Criteria 3: Not too short (avoid low-effort posts)
  const isSubstantial = ctx.post.text.length >= 20;

  // Criteria 4: Doesn't contain spam keywords
  const spamKeywords = ["crypto", "nft", "airdrop", "giveaway"];
  const isNotSpam = !spamKeywords.some(spam => text.includes(spam));

  // Combine all criteria
  const allowed = isFromConnection && hasInterestingTopic && isSubstantial && isNotSpam;

  Host.outputString(JSON.stringify(allowed));
}
