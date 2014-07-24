
const REQUEST = require("request");
const WAITFOR = require("waitfor");
const PARSE_LINK_HEADER = require("parse-link-header");


require("io.pinf.server.www").for(module, __dirname, function(app, config) {

	config = config.config;

    const DB_NAME = "devcomp";

    function callGithub(userInfo, path, callback) {
        var url = path;
        if (/^\//.test(url)) {
        	url = "https://api.github.com" + path;
        }
        return REQUEST({
            url: url,
            headers: {
                "User-Agent": "nodejs/request",
                "Authorization": "token " + userInfo.accessToken
            },
            json: true
        }, function (err, res, body) {
            if (err) return callback(err);
            if (res.statusCode === 403 || res.statusCode === 404) {
                console.error("Got status '" + res.statusCode + "' for url '" + url + "'! This is likely due to NOT HAVING ACCESS to this API call because your OAUTH SCOPE is too narrow! See: https://developer.github.com/v3/oauth/#scopes", res.headers);
                var scope = null;
/*
                if (/^\/orgs\/([^\/]+)\/teams$/.test(path)) {
                    scope = "read:org";
                } else
                if (/^\/teams\/([^\/]+)\/members\/([^\/]+)$/.test(path)) {
                    scope = "read:org";
                }
*/
                if (scope) {
                    console.log("We are going to start a new oauth session with the new require scope added ...");
                    var err = new Error("Insufficient privileges. Should start new session with added scope: " + scope);
                    err.code = 403;
                    err.requestScope = scope;
                    return callback(err);
                }
                return callback(new Error("Insufficient privileges. There should be a scope upgrade handler implemented for url '" + url + "'!"));
            }
        	if (res.headers.link) {
        		var link = PARSE_LINK_HEADER(res.headers.link);
        		if (link) {
        			if (!res.nav) res.nav = {}
        			if (link.prev) res.nav.prev = link.prev.url;
        			if (link.next) res.nav.next = link.next.url;
        		}
        	}
            return callback(null, res, body);
        });
    }

    function syncActivityForRepo(userInfo, r, owner, repo, callback) {

		console.log("Sync for repo:", owner, repo);

		return r.tableEnsure(DB_NAME, "myissues_collector_github", "repository_events", function(err, eventsTable) {
            if (err) return callback(err);

		    function syncIssues(callback) {
				return r.tableEnsure(DB_NAME, "myissues", "issues", {
					indexes: [
						"updatedOn"
					]
				}, function(err, issuesTable) {
		            if (err) return callback(err);

		            function getLastUpdatedTime(callback) {
		            	return issuesTable.orderBy({
		            		index: r.desc("updatedOn")
		            	}).filter({
		            		repository: "github.com/" + owner.name + "/" + repo.name
		            	}).limit(1).run(r.conn, function(err, cursor) {
		            		if (err) return callback(err);
							if (cursor.hasNext()) {
								return cursor.toArray(function(err, results) {
								    if (err) return callback(err);
								    return callback(null, results[0].updatedOn);
								});
							}
							return callback(null, null);
						});
		            }

            		function syncIssueForNumber(number, callback) {
			            return callGithub(userInfo, "/repos/" + owner.name + "/" + repo.name + "/issues/" + number, function(err, res, issue) {
			                if (err) return callback(err);
							return syncIssue(issue, callback);
			            });
            		}

					function syncIssue(issue, callback) {
						return issuesTable.insert({
	                		id: "github.com/" + owner.name + "/" + repo.name + "/issue/" + issue.id,
	                		repository: "github.com/" + owner.name + "/" + repo.name,
	                		externalUrl: issue.html_url,
	                		title: issue.title,
	                		state: issue.state,
							createdOn: new Date(issue.created_at).getTime(),
							updatedOn: new Date(issue.updated_at).getTime(),
							createdBy: "github.com/" + issue.user.login,
							assignedTo: (issue.assignee && issue.assignee.login && ("github.com/" + issue.assignee.login)) || null,
	                	}, {
	                        upsert: true
	                    }).run(r.conn, function (err, issueResult) {
	                        if (err) return callback(err);

	                        return syncIssueComments(issue.id, issue.comments_url, function(err) {
		                        if (err) return callback(err);

		                        return syncIssueEvents(issue.id, issue.events_url, function(err) {
			                        if (err) return callback(err);

			                        return callback(null, issueResult);
		                        });
	                        });
	                    });
					}

					function syncIssueComments(issueId, url, callback) {
			            return callGithub(userInfo, url, function(err, res, comments) {
			                if (err) return callback(err);
			                if (!comments || comments.length === 0) {
			                	return callback(null);
			                }
							return r.tableEnsure(DB_NAME, "myissues", "comments", function(err, commentsTable) {
					            if (err) return callback(err);
				                return commentsTable.insert(comments.map(function (comment) {
				                	return {
				                		id: "github.com/" + owner.name + "/" + repo.name + "/issue/" + issueId + "/comment/" + comment.id,
				                		repository: "github.com/" + owner.name + "/" + repo.name,
				                		issue: "github.com/" + owner.name + "/" + repo.name + "/issue/" + issueId,
				                		body: comment.body,
										createdOn: new Date(comment.created_at).getTime(),
										updatedOn: new Date(comment.updated_at).getTime(),
										createdBy: "github.com/" + comment.user.login
				                	};
			                	}), {
			                        upsert: true
			                    }).run(r.conn, function (err, result) {
						            if (err) return callback(err);

					                // TODO: Flag removed comments as removed but keep on our side.

									return callback(null);
				                });
							});
						});
					}

					function syncIssueEvents(issueId, url, callback) {
			            return callGithub(userInfo, url, function(err, res, events) {
			                if (err) return callback(err);
			                if (!events || events.length === 0) {
			                	return callback(null);
			                }
							return r.tableEnsure(DB_NAME, "myissues", "events", function(err, eventsTable) {
					            if (err) return callback(err);
				                return eventsTable.insert(events.map(function (event) {
				                	return {
				                		id: "github.com/" + owner.name + "/" + repo.name + "/issue/" + issueId + "/event/" + event.id,
				                		repository: "github.com/" + owner.name + "/" + repo.name,
				                		issue: "github.com/" + owner.name + "/" + repo.name + "/issue/" + issueId,
				                		event: event.event,
				                		// TODO: Record referenced commits.
//				                		commit: event.commit_id,
										createdOn: new Date(event.created_at).getTime(),
										createdBy: "github.com/" + event.actor.login
				                	};
			                	}), {
			                        upsert: true
			                    }).run(r.conn, function (err, result) {
						            if (err) return callback(err);
									return callback(null);
				                });
							});
						});
					}

		            return getLastUpdatedTime(function(err, lastUpdated) {
	            		if (err) return callback(err);

						if (!lastUpdated) {
							// We have no previous issues to we fetch them all.

							console.log("Fetch all issues for:", owner, repo);

				            function callPage(uri, callback, _pageIndex) {
				            	console.log("Calling page:", uri);
					            return callGithub(userInfo, uri, function(err, res, issues) {
					                if (err) return callback(err);
					                if (!issues || issues.length === 0) {
					                	return callback(null);
					                }
					                if (typeof issues === "object") {
					                	//	Match message 'Issues are disabled for this repo'
					                	if (/disabled/.test(issues.message)) {
					                		return callback(null);
							            }
					                }
									var loadNextPage = true;
					                var waitfor = WAITFOR.serial(function(err) {
					                	if (err) return callback(err);
					                	// We only fetch more issues if all issues were new.
					                	if (loadNextPage && res.nav && res.nav.next) {
				                			if (_pageIndex > 50) {
				                				console.error("WARNING: We will only go back 50 pages for now! Will not call:", res.nav.next);
				                				return callback(null);
				                			}
				                			return setTimeout(function() {
				                				return callPage(res.nav.next, callback, _pageIndex + 1);
				                			}, 1 * 1000);
				                		}
					                	return callback(null);
					                });
					                issues.forEach(function (issue) {
					                	return waitfor(function(callback) {
											return syncIssue(issue, function (err, result) {
						                        if (err) return callback(err);
						                        if (result.unchanged === 1) {
						                        	loadNextPage = false;
						                        }
						                        return callback(null);
						                    });
					                	});
					                });
									return waitfor();
					            });
				            }

				            return callPage("/repos/" + owner.name + "/" + repo.name + "/issues", callback, 1);
						}

						// Only fetch changed issues.
		            	return eventsTable.filter(
		            		r.row("org").eq(owner.id).and(r.row("repo").eq(repo.id)).and(r.row("time").gt(lastUpdated)).and(
		            			r.row("type").eq("IssuesEvent").or(r.row("type").eq("IssueCommentEvent"))
		            		)
						).run(r.conn, function(err, cursor) {
		            		if (err) return callback(err);
	            			if (!cursor.hasNext()) {
								console.log("No changes in issues for:", owner, repo);
	            				return callback(null);
	            			}
							return cursor.each(function(err, result) {
							    if (err) return callback(err);
								console.log("Sync issue '" + result.id + "' after change detected for:", owner, repo);
							    return syncIssueForNumber(result.issue.number, callback);
							});
		            	});
	            	});
            	});
		    }

	        function syncRepositoryEvents(callback) {

	            function didSyncBefore(callback) {
	            	return eventsTable.filter({
	            		org: owner.id,
	            		repo: repo.id
	            	}).limit(1).run(r.conn, function(err, cursor) {
	            		if (err) return callback(err);
						return callback(null, cursor.hasNext());	            		
	            	});
	            }

	            return didSyncBefore(function(err, didSyncBefore) {
	            	if (err) return callback(err);

		            function callPage(uri, callback, _pageIndex) {
		            	console.log("Calling page:", uri);
			            return callGithub(userInfo, uri, function(err, res, events) {
			                if (err) return callback(err);

			                var loadNextPage = true;

			                var waitfor = WAITFOR.serial(function(err) {
			                	if (err) return callback(err);
			                	// We only load older pages if we synced before and should catch up.
								if (loadNextPage && res.nav) {
			                		if (didSyncBefore) {
				                		if (res.nav.next) {
				                			if (_pageIndex > 25) {
				                				console.error("WARNING: We will only go back 25 pages for now! Will not call:", res.nav.next);
				                				return callback(null);
				                			}
				                			return setTimeout(function() {
				                				return callPage(res.nav.next, callback, _pageIndex + 1);
				                			}, 1 * 1000);
				                		}
			                		} else {
			                			console.log("Fecth no more pages for", owner, repo, "as this is the first fetch!");
			                		}
			                	}
			                	return callback(null);
			                });

			                events.reverse();
			                events.forEach(function (evt) {
			                	var info = {
			                		id: evt.id,
			                		type: evt.type,
			                		actor: null,
			                		repo: null,
			                		org: null,
			                		issue: null,
			                		created: null,
			                		pushed: null,
			                		time: new Date(evt.created_at).getTime()
			                	};
			                	if (evt.actor) {
			                		info.actor = evt.actor.id;
			                	}
			                	if (evt.repo) {
			                		info.repo = evt.repo.id;
			                	}
			                	if (evt.org) {
			                		info.org = evt.org.id;
			                	}
			                	if (evt.type === "IssueCommentEvent") {
			                		info.issue = {
			                			id: evt.payload.issue.id,
			                			number: evt.payload.issue.number,
			                			action: evt.payload.action,
			                			comment: evt.payload.comment.id
			                		};
			                	} else
			                	if (evt.type === "IssuesEvent") {
			                		info.issue = {
			                			id: evt.payload.issue.id,
			                			number: evt.payload.issue.number,
			                			action: evt.payload.action
			                		};
			                	} else
			                	if (evt.type === "CreateEvent") {
			                		info.created = {
			                			ref: evt.payload.ref,
			                			ref_type: evt.payload.ref_type,
			                			master_branch: evt.payload.master_branch
			                		};
			                	} else
			                	if (evt.type === "PushEvent") {
			                		info.pushed = {
			                			id: evt.payload.push_id,
			                			ref: evt.payload.ref,
			                			head: evt.payload.head
			                		};
			                	} else {
			                		console.log("Ignoring event: " + evt.type);
			                		return;
			                	}

			                	return waitfor(function(callback) {
									return eventsTable.insert(info, {
				                        upsert: true
				                    }).run(r.conn, function (err, result) {
				                        if (err) return callback(err);
				                        if (result.inserted === 1) {
					                		console.log("Inserted into DB:", JSON.stringify(info));
										}
				                        if (result.unchanged === 1) {
				                        	loadNextPage = false;
				                        }
				                        return callback(null);
				                    });
			                	});
			                });

							return waitfor();
			            });
		            }

		            return callPage("/repos/" + owner.name + "/" + repo.name + "/events", callback, 1);
	            });
	        }

	        return syncRepositoryEvents(function(err) {
	        	if (err) return callback(err);
	        	return syncIssues(callback);
	        });
        });       
    }


    function syncActivity(userInfo, r, callback) {
    	if (!config.watch) {
    		console.log("No repositories to watch configured!");
    		return callback(null);
    	}
		return r.tableEnsure(DB_NAME, "myissues", "repositories", function(err, repositoryTable) {
            if (err) return callback(err);
	        var waitfor = WAITFOR.parallel(callback);
	        for (var orgName in config.watch.organizations) {
	        	waitfor(orgName, function (orgName, callback) {
		            return callGithub(userInfo, "/orgs/" + orgName + "/repos", function(err, res, repos) {
		                if (err) return callback(err);
				        var waitfor = WAITFOR.parallel(callback);
		                repos.forEach(function(repo) {
//repos.slice(0, 1).forEach(function(repo) {
				        	waitfor(function(callback) {
								return repositoryTable.insert({
			                		id: "github.com/" + repo.full_name,
			                		private: repo.private,
			                		description: repo.description,
			                		homepage: repo.homepage
			                	}, {
			                        upsert: true
			                    }).run(r.conn, function (err, result) {
			                        if (err) return callback(err);
									return syncActivityForRepo(userInfo, r, {
										id: repo.owner.id,
										name: repo.owner.login
									}, {
										id: repo.id,
										name: repo.name
									}, callback);
			                    });	
				        	});
		                });
				        return waitfor();
		            });
	        	});
	        }
	        return waitfor();
	    });
    }


    var triggerSync = null;
    var isSyncing = false;

    function startRegularActivitySync(res) {

		triggerSync = function () {
			if (isSyncing) {
				console.log("Skip sync trigger. Already syncing.");
				return;
			}
			isSyncing = true;
			console.log("Trigger activity sync with access token from user:", res.view.authorized.github.username);
			return syncActivity(res.view.authorized.github, res.r, function (err) {
				isSyncing = false;
				if (err) {
					console.error("Error syncing activity", err.stack);
				}
				console.log("Sync done");
				return;
	        });
		}

		setInterval(function () {
			return triggerSync();
		}, 60 * 5 * 1000);

		return triggerSync();
    }


    var credentialsEnsured = false;

    app.get(/\/sync\/now$/, function(req, res, next) {
        console.log("Sync now triggered!");
        if (!triggerSync) {
        	console.log("'triggerSync' not set!");
        	res.writeHead(500);
            return res.end("'triggerSync' not set!");
        }
        triggerSync();
        return res.end();
    });

    app.get(/\/ensure\/credentials$/, function(req, res, next) {

        console.log("Ensure credentials triggered");

        function respond (payload) {
            payload = JSON.stringify(payload, null, 4);
            res.writeHead(200, {
                "Content-Type": "application/json",
                "Content-Length": payload.length,
                "Cache-Control": "max-age=15"  // seconds
            });
            return res.end(payload);
        }

        if (credentialsEnsured) {
            return respond({
                "$status": 200
            });
        }

        if (!res.view || !res.view.authorized) {
            return respond({
                "$status": 403,
                "$statusReason": "No user authorized!"
            });
        }
        if (!res.view.authorized.github) {
            return respond({
                "$status": 403,
                "$statusReason": "No github user authorized!"
            });
        }

        console.log("res.view.authorized", JSON.stringify(res.view.authorized, null, 4));
/*
        if (
            res.view.authorized.github.scope.indexOf("write:repo_hook") === -1 ||
            res.view.authorized.github.scope.indexOf("repo") === -1
        ) {
            var scope = "write:repo_hook,repo";

            return respond({
                "$status": 403,
                "$statusReason": "Insufficient scope",
                "requestScope": scope
            });
        }
*/
        startRegularActivitySync(res);

        credentialsEnsured = true;

        return respond({
            "$status": 200
        });
    });

});
