#!/usr/bin/env node
/**
 * Script to replace console.log/error with logger calls in importer and CLI files
 */
const fs = require('fs');
const path = require('path');

// Files to process
const files = [
    'importers/yahoo.importer.js',
    'importers/reuters.importer.js',
    'importers/xe.importer.js',
    'main.js',
    'cli/jobs.js'
];

function replaceConsoleCalls(content) {
    // Replace console.log with template literals
    content = content.replace(/console\.log\(`([^`]+)`\);/g, (match, msg) => {
        // Extract variables from template string
        const hasVariables = msg.includes('${');
        if (hasVariables) {
            return `logger.info(\`${msg}\`);`;
        }
        return `logger.info('${msg}');`;
    });
    
    // Replace console.log with string literals
    content = content.replace(/console\.log\('([^']+)'\);/g, "logger.info('$1');");
    content = content.replace(/console\.log\("([^"]+)"\);/g, 'logger.info("$1");');
    
    // Replace console.error with template literals
    content = content.replace(/console\.error\(`([^`]+)`\);/g, "logger.error(`$1`);");
    
    // Replace console.error with  string literals
    content = content.replace(/console\.error\('([^']+)'\);/g, "logger.error('$1');");
    content = content.replace(/console\.error\("([^"]+)"\);/g, 'logger.error("$1");');
    
    // Replace console.error with error object
    content = content.replace(/console\.error\('([^']*)',\s*error\);/g, "logger.error({ error: error.message, stack: error.stack }, '$1');");
    content = content.replace(/console\.error\('([^']*)',\s*error\.message\);/g, "logger.error({ error: error.message }, '$1');");
    content = content.replace(/console\.error\(`([^`]*)`,\s*error\);/g, "logger.error({ error: error.message, stack: error.stack }, `$1`);");
    content = content.replace(/console\.error\(`([^`]*)`,\s*error\.message\);/g, "logger.error({ error: error.message }, `$1`);");
    
    return content;
}

files.forEach(filePath => {
    const fullPath = path.join(__dirname, '..', filePath);
    
    if (!fs.existsSync(fullPath)) {
        console.log(`⊘ File not found: ${fullPath}`);
        return;
    }
    
    const originalContent = fs.readFileSync(fullPath, 'utf-8');
    const content = replaceConsoleCalls(originalContent);
    
    if (content !== originalContent) {
        fs.writeFileSync(fullPath, content, 'utf-8');
        console.log(`✓ Updated: ${filePath}`);
    } else {
        console.log(`- No changes: ${filePath}`);
    }
});

console.log('\nDone!');

