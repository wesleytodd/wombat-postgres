// Requirements
var util = require('util'),
	async = require('async'),
	pg = require('pg'),
	_ = require('lodash');

module.exports = function(wombat) {

	var PostgreSQL = function(options) {
		// Merge options
		this.options = _.extend({}, PostgreSQL.defaultOptions, this.options || {}, options);

		// Create service
		this.service = new wombat.Service('postgresql');

		// Create package
		this.package = new wombat.Package(util.format('postgresql-%s', this.options.version), {
			otherPackages: {
				'postgresql-contrib': !!this.options.installContrib
			}
		});

		// Keep connected state
		this.connected = false;

		// Create client
		this.client = new pg.Client({
			user: this.options.defaultUser,
			password: this.options.defaultPassword,
			database: this.options.defaultDatabase,
			port: this.options.port,
		});

		// Close on error
		this.client.on('error', function(err) {
			wombat.logger.log('error', err);
		});

	};
	util.inherits(PostgreSQL, wombat.Package);

	// Defaults
	PostgreSQL.defaultOptions = {
		installContrib: true,
		defaultUser: 'postgres',
		defaultPassword: 'postgres',
		defaultDatabase: 'postgres',
		version: '9.1',
		port: 5432,
		listenAddresses: 'localhost',
		maxConnections: 100,
		socket: '/var/run/postgresql',
		logPrefix: '%t ',
		users: [{
			type: 'host',
			database: 'all',
			name: 'all',
			address: '10.0.2.2/24',
			method: 'trust'
		}]
	};

	// Connect to the postgres client
	PostgreSQL.prototype.openConnection = function(done) {
		if (!this.connected) {
			// Connection
			this.client.connect(function(err) {
				if (err) {
					wombat.logger.log('error', err);
				}
				this.connected = true;
				if (typeof done === 'function') {
					done(err);
				}
			}.bind(this));
		}
	};

	PostgreSQL.prototype.closeConnection = function() {
		if (this.connected) {
			this.connected = false;
			this.client.end();
		}
	};

	// Copy configs
	PostgreSQL.prototype.copyConfigs = function(done) {
		async.parallel([function(done) {
			// Main conf
			var p = path.resolve(path.sep + path.join('etc', 'postgresql', this.options.version, 'main', 'postgresql.conf'));
			wombat.template(path.join(__dirname, 'templates', 'postgresql.conf.tmpl'), p, this.options, done);
		}.bind(this), function(done) {
			// Users conf
			var p = path.resolve(path.sep + path.join('etc', 'postgresql', this.options.version, 'main', 'pg_hba.conf'));
			wombat.template(path.join(__dirname, 'templates', 'pg_hba.conf.tmpl'), p, this.options, done);
		}.bind(this)], done);
	};

	// Install the package
	PostgreSQL.prototype.install = function(done) {
		this.package.install(function(err) {
			// Fail on error
			if (err) {
				wombat.logger.log('error', 'Failed to install PostgreSQL');
				return done(err);
			}

			var cmd = util.format('sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD \'%s\';"', this.options.defaultPassword);
			wombat.exec(cmd, function(err) {
				// Fail on error
				if (err) {
					wombat.logger.log('error', 'Failed to install PostgreSQL');
					return done(err);
				}

				// Successfully completed
				wombat.logger.log('info', 'PostgreSQL Installed.');
				done();
			});

		}.bind(this));

	};

	// Create a user
	PostgreSQL.prototype.user = function(name, pass, done) {

		// Make sure we are connected
		this.openConnection();

		// check if user exists
		this.client.query(util.format('SELECT 1 FROM pg_roles WHERE rolname=\'%s\'', name), function(err, res) {
			// warn on error, but dont quit
			if (err) {
				wombat.logger.log('warn', err);
			}

			// Create if not exists
			if (res.rowCount != 1) {

				// Create query
				var q = util.format('CREATE USER %s WITH PASSWORD \'%s\';', name, pass);
				wombat.logger.log('verbose', 'running query: ', q);

				// Run query
				this.client.query(q, function(err) {
					// Log error
					if (err) {
						wombat.logger.log('error', err);
						return done(err);
					}

					// Log success
					wombat.logger.log('info', 'PostgreSQL user "%s" created with password "%s".', name, pass);
					done();
				}.bind(this));

			} else {
				// Already exists
				wombat.logger.log('info', util.format('Postgres user %s already exists', name));
				done();
			}
			
		}.bind(this));

	};

	// Create a database
	PostgreSQL.prototype.database = function(name, options, done) {

		// Default options
		if (typeof options === 'function') {
			done = options;
			options = {};
		}

		// Check if database exists
		wombat.exec(util.format('sudo -u postgres psql -l | grep %s | wc -l', name), function(err, out) {
			// Log error, but continute
			if (err) {
				wombat.logger('Error checking if database exists.');
			}

			// Create if it doesnt already exist
			if (out.trim() != '1') {

				// Make sure we are connected
				this.openConnection();

				// Build query
				var q = 'CREATE DATABASE ' + name;
				_.each(options, function(v, k) {
					q += ' ' + k + ' ' + v;
				});
				q += ';';
				wombat.logger.log('verbose', 'running query: ', q);

				// Create the user
				this.client.query(q, function(err) {
					// Log error
					if (err) {
						wombat.logger.error(err);
						return done(err);
					}

					// Log success
					wombat.logger.log('verbose', 'Database %s created.', name);
					done();
				}.bind(this));
			} else {
				// Already exists
				wombat.logger.log('info', util.format('Postgres datatabase %s already exists.', name));
				done();
			}

		}.bind(this));
	
	};

	// Return a new instance of this plugin
	return PostgreSQL;
};
