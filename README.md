# GOB-Config

Holds the configuration for the Amsterdam GOB implementation.

# How to update

GOB-Config is used by GOB-Import, GOB-Export, GOB-Prepare, GOB-StUF and GOB-Test.

If any changes to GOB-Config have been made, tested and accepted,
the first step is to define a new release for GOB-Config.

https://docs.github.com/en/enterprise/2.13/user/articles/creating-releases

Simply add one to the existing version number,
so for example v0.4.11 becomes v0.4.12

Characters are only used for releases that are under development (eg v0.4.11a),
so use them for draft releases and don't use them for final releases.

The next step is to update the requirements for the GOB repositories that use GOB-Config.

For each repository:
- create a new branch
- update src/requirements.txt for the new version, eg:  
-e git+https://github.com/Amsterdam/GOB-Config.git@v0.4.11j#egg=gobconfig becomes  
-e git+https://github.com/Amsterdam/GOB-Config.git@v0.4.12#egg=gobconfig
- Create a pull request
- When the pull request has been accepted, rebased and merged:  
    - create a pull request for develop to master
    - When this pull request has been accepted and merged the code will be available in the acceptance environment.
