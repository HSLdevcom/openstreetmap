// assign explict popularity values by OSM source identifier
// values below the default 10 will lower the ranking
// values above 10 will boost the ranking
// currently this works mainly amnong equally good matches

module.exports = {
  'way:425291777': 1, // hki-vantaa lentoasema, undesired point inside terminal
  'node:4625883091': 11 // HSL customer service wins HSL main office
};
