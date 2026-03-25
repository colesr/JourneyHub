const {setGlobalOptions} = require("firebase-functions");
const {onCall, HttpsError} = require("firebase-functions/v2/https");
const {defineSecret} = require("firebase-functions/params");
const {GoogleGenerativeAI} = require("@google/generative-ai");

setGlobalOptions({maxInstances: 10});

const geminiApiKey = defineSecret("GEMINI_API_KEY");

// Summarize a thread and its comments
exports.summarizeThread = onCall(
    {secrets: [geminiApiKey]},
    async (request) => {
      if (!request.auth) {
        throw new HttpsError("unauthenticated", "Must be logged in.");
      }

      const {title, content, comments} = request.data;
      if (!title) {
        throw new HttpsError("invalid-argument", "Thread title is required.");
      }

      const genAI = new GoogleGenerativeAI(geminiApiKey.value());
      const model = genAI.getGenerativeModel({model: "gemini-2.0-flash"});

      let prompt = `Summarize this forum thread concisely in 2-3 sentences.\n\n`;
      prompt += `Title: ${title}\n`;
      if (content) prompt += `Post: ${content}\n`;
      if (comments && comments.length > 0) {
        prompt += `\nComments:\n`;
        for (const c of comments) {
          prompt += `- ${c.author}: ${c.content}\n`;
        }
      }

      const result = await model.generateContent(prompt);
      return {summary: result.response.text()};
    },
);

// Help improve a comment draft
exports.improveComment = onCall(
    {secrets: [geminiApiKey]},
    async (request) => {
      if (!request.auth) {
        throw new HttpsError("unauthenticated", "Must be logged in.");
      }

      const {draft, threadTitle, threadContent} = request.data;
      if (!draft) {
        throw new HttpsError("invalid-argument", "Comment draft is required.");
      }

      const genAI = new GoogleGenerativeAI(geminiApiKey.value());
      const model = genAI.getGenerativeModel({model: "gemini-2.0-flash"});

      let prompt = `You are a helpful writing assistant for a forum. `;
      prompt += `Improve this comment to be clearer and more constructive, `;
      prompt += `but keep the original meaning and tone. Keep it concise. `;
      prompt += `Only return the improved comment text, nothing else.\n\n`;
      prompt += `Thread: ${threadTitle}\n`;
      if (threadContent) prompt += `Thread content: ${threadContent}\n`;
      prompt += `\nOriginal comment: ${draft}`;

      const result = await model.generateContent(prompt);
      return {improved: result.response.text()};
    },
);
