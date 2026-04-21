// Tiny static file server for JourneyHub on Railway
const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// Serve static files from the project root, but ignore folders we don't want exposed
app.use(express.static(__dirname, {
  index: 'index.html',
  extensions: ['html'],
  setHeaders: (res, filePath) => {
    // Cache static assets, not the HTML
    if (filePath.endsWith('.html')) {
      res.setHeader('Cache-Control', 'no-cache');
    } else {
      res.setHeader('Cache-Control', 'public, max-age=3600');
    }
  }
}));

// SPA fallback — every unknown route serves index.html
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

app.listen(PORT, () => {
  console.log(`JourneyHub static server running on port ${PORT}`);
});
