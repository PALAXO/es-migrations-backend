import nconf from 'nconf';

nconf.env();
nconf.argv();
nconf.file(`conf`, { file: `${import.meta.dirname}/config.json` });

export default nconf;
