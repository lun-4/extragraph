// Simple keyword filter example in JavaScript
// This filter includes posts that contain specific keywords

// Export the filter function
export function filter() {
  // Read the input JSON from Extism
  const input = Host.inputString();
  const ctx = JSON.parse(input);

  // Define keywords to filter for
  const keywords = ["golang", "rust", "wasm", "bluesky"];

  // Check if post exists and contains any of our keywords
  let allowed = false;

  if (ctx.post && ctx.post.text) {
    const text = ctx.post.text.toLowerCase();
    allowed = keywords.some(keyword => text.includes(keyword));
  }

  // Return the result as JSON
  Host.outputString(JSON.stringify(allowed));
}
