minor:
	bumpversion minor --allow-dirty
	git push
	git push --tags
patch:
	bumpversion patch --allow-dirty
	git push
	git push --tags
