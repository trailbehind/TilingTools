#!/usr/bin/env node
/*
Reads a GeoJSON file and outputs the same file with HTML encodes stripped
*/
const fs = require('fs');
const readline = require('readline');
const he = require('he');

// Check if a file path was passed - one is required
const args = process.argv;
if (args.length < 3) {
    console.log('Error: Please provide a path to a GeoJSON file');
    process.exit(1);
}
if (args.length > 4) {
    console.log('Error: Too many arguments. Please provide a path to a GeoJSON file and an optional output path');
    process.exit(1);
}
const inputFile = args[2]
const outputFile = args[3] || outputFilename(inputFile);

if (!(fs.existsSync(inputFile))) {
    console.log(`Error: The input file ${inputFile} does not exist`);
    process.exit(1);
}
if (fs.existsSync(outputFile)) {
    console.log(`Error: The output file ${inputFile} already exists. Please move or delete it before continuing`);
    process.exit(1);
}

// Create a read stream for the file
const lineReader = readline.createInterface({
    input: fs.createReadStream(inputFile, { encoding: 'utf8' })
});

// Create a write stream for the output
const writeStream = fs.createWriteStream(outputFile, { flags : 'w' });

let lines = 0;
lineReader.on('line', line => {
    // Remove trailing comma
    let parsed = line.trim().replace(/(,$)/g, '')
    try {
        parsed = JSON.parse(parsed)
        let newProperties = {}
        Object.keys(parsed.properties).forEach(key => {
            if (parsed.properties[key]) {
                newProperties[key] = he.decode(parsed.properties[key].replace(/\r?\n|\r/g, ''))
            }

        })
        parsed.properties = newProperties

        writeStream.write(((lines > 0) ? ',' : '') + JSON.stringify(parsed))
        lines++
    } catch(e) {
        writeStream.write(line)
    }
});

function outputFilename(name) {
    const parts = name.split('.')
    return `${parts[0]}-cleaned.${parts[1] || '.geojson'}`;
}
