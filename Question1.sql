SELECT git_branch, gh_team_size, gh_project_name, git_num_committers, gh_first_commit_created_at limit 5
FROM travistorrent_27_10_2016_bk 
WHERE git_branch IN
(SELECT distinct(git_branch)
	FROM travistorrent_27_10_2016_bk 
	WHERE tr_status = 'fail' OR tr_status = 'errored')
;
