#!/usr/bin/python

r"""GRIN/OAUTH2 python example.

INITIAL SETUP
-------------

As a prerequisite, make sure you have the python oauth2 client installed:

 0) $ sudo apt install python-oauth2client
      (or)
    $ sudo -H pip install oauth2client
      (or the equivelant for your distribution)

To use this script, you must identify a google account you'd like to use to
access GRIN, and then jump through some hoops to allow a program to use it.
You may want to create a new account to do this. You'll need to share the
email address for this account with Ben.

Once Ben has whitelisted the account, you can try to use it to access grin.
First, login to that account in a browser window. You may want to use an
incognito window.

 1) Go to 'https://console.developers.google.com' and create a new project.
 2) Click the 'Credentials' tab.
 3) Select 'OAuth client ID' in the credential type dropdown.
 4) Click 'Configure consent screen'.
 5) In the 'Product name' box, put something like 'Scripted Access to GRIN'.
    (whatever you want is fine)
 6) Click 'Save'.
 7) You should now see a radio list titled 'Application type'. Click 'Other'.
    Put in whatever name you'd like. Click 'Create'.
 8) You'll see a dialog box that has 'Here is your client ID' and 'Here is
    your client secret' boxes. Ignore this, just click 'OK'.
 9) Now, click the 'download' button to the right of the credentials you just
    created. The button is a down arrow with a line under it.
 10) Move that file into the same directory as this script, and call it
     '.secrets'.

Now you can use this script to generate credentials that will work with GRIN.
You can also use it to fetch things from GRIN, if that fits your workflow.
First, you must understand a little bit about what is in the '.secrets' file.

This file identifies the 'project' you created in the developer console, and
as such, it allows a program to request that a user give it permission to do
whatever you've allowed in that project. In our case, GRIN just needs to
identify you, and so we have not enabled any special APIs (like gmail) that
would give somebody access to your personal things.

The first thing we need to do is to generate credentials that can be shared
with GRIN to act under the permissions of that project. Run this program:

 $ ./grin_oauth.py --noauth_local_webserver --directory UOM   # use your dir
 <expected program output follows:>
 Go to the following link in your browser:

     https://accounts.google.com/o/oauth2/auth?...
     <go to this link in your webbrowser. sign in to the account you want to
      authorize. click 'allow' or 'accept' or whatever, when it prompts you
      to give permissions>

 Enter verification code: <copy the code that's displayed and paste it here>
 Authentication successful.
  ....
 $

This should now have dumped out your summary CSV. If you see that, you're in
good shape. If you run the command a second time, you shouldn't be prompted
for anything.

Now you have a new file, '.creds', which contains the access credentials that
are shared with GRIN. It is this token that GRIN uses to authenticate you. You
will want to protect this file - while it does not enable anyone to read your
email, for example, it could be used to share your email address and profile
information.


USING THE TOOL
--------------

You can use this script to fetch something from GRIN, or you can use it to
just output the header you need to include in your requests. Use whatever
works best for your workflow. Here are examples of both:

JUST LOGGING IN:
----------------

$ ./grin_oauth.py --just-output-bearer-token
Authorization: Bearer ya29.GlvqA1bT...
$

This will renew your token and output the header you need to include when
fetching things from GRIN. This lets you use common tools like CURL.

$ curl -H "$(./grin_oauth.py --just-output-bearer-token)" \
    https://books.google.com/libraries/UOM/?format=text

FETCHING THINGS:
----------------

$ ./grin_oauth.py --directory UOM \
     --resource '_process?barcodes=39015093756719'
Barcode  Status
39015093756719 Already being converted
$ ./grin_oauth.py --directory UOM --resource 35128000678456.tar.gz.gpg \
     -o encrypted.tgz.gpg

WRITING YOUR OWN PROGRAM:
-------------------------

You might write your own python script using this module.

cat << __DONE__ > example_script.py
import grin_oauth
creds = grin_oauth.CredentialsFactory('.creds')
r = grin_oauth.MakeGrinRequest(creds, (
  'https://books.google.com/libraries/UOM/_circulation_reports?format=text'))
print r.read()
__DONE__
"""

from __future__ import print_function
import argparse
import sys
import urllib2
import httplib2

from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import argparser
from oauth2client.tools import run_flow

# These are the authentication scopes you are requesting. These are the limited
# powers granted to the thing you give the token to. In this case, you're asking
# for a token that will give GRIN the permission to see your email address and
# profile information.
SCOPES = ['https://www.googleapis.com/auth/userinfo.email',
          'https://www.googleapis.com/auth/userinfo.profile']

# This is the file you downloaded from console.developers.google.com when you
# created your 'project'. You need this to generate credentials. Once you've
# generated the credentials, you could delete this file.
SECRETS_FILE = '/opt/digiverso/goobi/scripts/googlebooks/.secrets'

# This file contains the authorization token ('access_token') shared with GRIN,
# and the refresh token ('refresh_token') used to issue access tokens when your
# current token has expired.
CREDENTIALS_FILE = '/opt/digiverso/goobi/scripts/googlebooks/.creds'

# How much we read/write when streaming repsonse data.
OUTPUT_BLOCKSIZE = 1024 * 1024


class CredsMissingError(IOError):
  """Raised by CredentialsFactory() when credentials are missing."""


class GRINPermissionDeniedError(IOError):
  """GRIN says you're not allowed."""


class GoogleLoginError(IOError):
  """Something failed logging in to Google."""


def CredentialsFactory(credentials_file):
  """Use the oauth2 libraries to load our credentials."""
  storage = Storage(credentials_file)
  creds = storage.get()
  if creds is None:
    raise CredsMissingError()

  # If our credentials are expired, use the 'refresh_token' to generate a new
  # one.
  if creds.access_token_expired:
    creds.refresh(httplib2.Http())
  return creds


def MakeGrinRequest(creds, url):
  """Makes an HTTP request to grin using urllib2, and returns the response."""
  request = urllib2.Request(url)
  request.add_header('Authorization', 'Bearer %s' % creds.access_token)

  try:
    response = urllib2.urlopen(request)

  # Try to give better diagnostics on a 403, which means we are logged in OK but
  # GRIN denied the request.
  except urllib2.HTTPError, exc:
    if exc.code == 403:
      raise GRINPermissionDeniedError((
          'GRIN denied this request (403). You may not have permission to '
          'access this directory, or we may not have applied your ACL to '
          'production, yet. This can take up to 12 hours.'))
    raise exc

  # See if we were redirected to the account login page. This means something
  # about our token was not accepted. Our project may not be setup correctly.
  if 'accounts.google.com/ServiceLogin' in response.geturl():
    raise GoogleLoginError((
        'Something went wrong logging in to Google. Your credentials file '
        'may be invalid.'))
  return response


def main():
  # Parse input arguments - the run_flow() method in the OAuth2 library is
  # customized with some command line arguments.
  #
  # We can also add our own arguments.
  parser = argparse.ArgumentParser(
      description=__doc__,
      formatter_class=argparse.RawDescriptionHelpFormatter,
      parents=[argparser])
  parser.add_argument('-u', '--base_url',
                      default='https://books.google.com/libraries/',
                      help='Base URL for GRIN access.')
  parser.add_argument('-d', '--directory', default='UOM',
                      help='The library directory to work with on GRIN.')
  parser.add_argument('-r', '--resource', default='?format=text',
                      help=('The resource to fetch from the directory. By '
                            'default this is the CSV version of the summary '
                            'page.'))
  parser.add_argument('--just-output-bearer-token', action='store_true',
                      help=('Instead of fetching anything from GRIN, just '
                            'output a valid bearer token to STDOUT and then '
                            'exit.'))
  parser.add_argument('-o', '--output-file', default='/dev/stdout',
                      help='Where to write output.')
  parser.add_argument('--secrets-file', default=SECRETS_FILE,
                      help='Secrets downloaded from your developer project.')
  parser.add_argument('--credentials-file', default=CREDENTIALS_FILE,
                      help='Where to store OAUTH2 credentials.')
  flags = parser.parse_args()

  # Get proper oauth2 credentials.
  try:
    creds = CredentialsFactory(flags.credentials_file)
  except CredsMissingError:
    # Use the oauth2 flow to do an initial login with a secrets file.
    storage = Storage(flags.credentials_file)
    creds = run_flow(flow_from_clientsecrets(flags.secrets_file, scope=SCOPES),
                     storage, flags)

  # If we're just being used to renew/fetch the secret, go ahead and output it
  # and stop.
  if flags.just_output_bearer_token:
    print('Authorization: Bearer %s' % creds.access_token)
    return

  # Format the URL we want to fetch, and fetch it.
  url = flags.base_url + flags.directory + '/' + flags.resource
  response = MakeGrinRequest(creds, url)

  # Read/write in this block size.
  with open(flags.output_file, 'w') as f:
    while True:
      stuff = response.read(OUTPUT_BLOCKSIZE)
      if not stuff:
        break
      f.write(stuff)


if __name__ == '__main__':
  main()
