// Requirements
var util = require('util'),
	pg = require('pg'),
	_ = require('lodash');

module.exports = function(wombat) {

	var PostgreSQL = function(conf) {
		// Merge options
		this.options = _.extend(this.options || {}, PostgreSQL.defaultOptions, conf);

		// Extends service and package
		wombat.Service.call(this, 'postgresql');
		wombat.Package.call(this, 'postgresql', {
			otherPackages: {
				'postgresql-contrib': !!this.options.installContrib
			}
		});

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
		}.bind(this));

		// Close on drain
		//this.client.on('drain', function() {
		//	wombat.logger.log('verbose', 'Disconnecting from database');
		//	this.client.end();
		//}.bind(this));

		// Connection
		this.client.connect(function(err) {
			if (err) {
				wombat.logger.log('error', err);
			}
		});
	};
	util.inherits(PostgreSQL, wombat.Service);
	util.inherits(PostgreSQL, wombat.Package);

	// Defaults
	PostgreSQL.defaultOptions = {
		installContrib: true,
		defaultUser: 'postgres',
		defaultPassword: 'postgres',
		defaultDatabase: 'postgres',
		port: 5432
	};

	// Install the package
	PostgreSQL.prototype.install = function(done) {
		wombat.Package.prototype.install.call(this, function(err) {
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

	};

	// Create a database
	PostgreSQL.prototype.database = function(name, options, done) {

		// Default options
		if (typeof options === 'function') {
			done = options;
			options = {};
		}

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
				console.log(arguments);
				wombat.logger.error(err);
				return done(err);
			}

			// Log success
			wombat.logger.log('verbose', 'Database %s created.', name);
			done();
		}.bind(this));
	
	};

	// Return a new instance of this plugin
	return PostgreSQL;
};
