# geojson-strip-html
A utility for removing HTML character entities from the `properties` of a GeoJSON file.

## Installation
There is a single dependency (`he`) that must be installed:

````bash
npm install
````

## Use
The script expects a valid line-delimited GeoJSON file, and optional output file name and path.

````bash
node index.js land.geojson
````

The above example will output a file `land-cleaned.geojson`. You can also specify an alternate outfile file name and location:

````bash
node index.js land.geojson ~/land-no-html.geojson
````

