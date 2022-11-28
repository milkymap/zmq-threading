import click 

from log import logger 
from commands import concurrent_server, concurrent_runner

@click.group(chain=False, invoke_without_command=True)
@click.pass_context
def command_line_interface(ctx:click.core.Context):
    ctx.ensure_object(dict)
    subcommand = ctx.invoked_subcommand
    if subcommand is not None: 
        logger.debug(f'{subcommand} was called')
    

command_line_interface.add_command(
    cmd=concurrent_server, 
    name='server-mode'
)

command_line_interface.add_command(
    cmd=concurrent_runner, 
    name='runner-mode'
)
if __name__ == '__main__':
    command_line_interface(obj={})