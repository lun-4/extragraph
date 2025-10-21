// Filter that only shows posts from people you follow
// This demonstrates using the "follows" relationship data

export function filter() {
  const input = Host.inputString();
  const ctx = JSON.parse(input);

  // Only include posts from people we follow
  let allowed = false;

  if (ctx.post && ctx.author_did) {
    // Check if author is in our follows list
    allowed = ctx.author_did in ctx.follows;
  }

  Host.outputString(JSON.stringify(allowed));
}
