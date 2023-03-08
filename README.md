# GOB-Config

Holds the configuration for the Amsterdam GOB implementation.

# How to update

GOB-Config is used by [GOB-Import](https://github.com/Amsterdam/GOB-Import), [GOB-Export](https://github.com/Amsterdam/GOB-Export), [GOB-Prepare](https://github.com/Amsterdam/GOB-Prepare), [GOB-StUF](https://github.com/Amsterdam/GOB-StUF) and [GOB-Test](https://github.com/Amsterdam/GOB-Test).

If any changes to GOB-Config have been made, tested and accepted,
the first step is to define a new release for GOB-Config.

https://docs.github.com/en/enterprise/2.13/user/articles/creating-releases

Simply add one to the existing version number,
so for example v0.8.16 becomes v0.8.17.

Characters are only used for releases that are under development (eg v0.4.11a),
so use them for draft releases and don't use them for final releases.

The next step is to update the requirements for the GOB repositories that use GOB-Config.

For each repository:

- Create a new branch
- Update `src/requirements.txt` for the new version, e.g.:
`gobconfig@git+https://github.com/Amsterdam/GOB-Config.git@v0.8.16` becomes
`gobconfig@git+https://github.com/Amsterdam/GOB-Config.git@v0.8.17`
- Create a pull request
- When the pull request has been accepted, rebased and merged:  
    - Create a pull request for *develop* to *master*
    - When this pull request has been accepted and merged the code will be available in the acceptance environment.
