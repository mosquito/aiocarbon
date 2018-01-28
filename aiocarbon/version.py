author_info = (
    ('Dmitry Orlov', 'me@mosquito.su'),
)

project_home = 'http://github.com/mosquito/aiocarbon'

package_info = "Asynchronous client for carbon."
package_license = "Apache 2"

team_email = 'me@mosquito.su'

version_info = (0, 1, 0)

__author__ = ", ".join("{} <{}>".format(*info) for info in author_info)
__version__ = ".".join(map(str, version_info))
