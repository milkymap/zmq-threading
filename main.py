import click 

from strategies import start_worker
from log import logger 

@click.group(chain=False, invoke_without_command=True)
@click.pass_context
def command_line_interface(ctx:click.core.Context):
    ctx.ensure_object(dict)
    subcommand = ctx.invoked_subcommand
    if subcommand is not None: 
        logger.debug(f'{subcommand} was called')
    

command_line_interface.add_command(
    cmd=start_worker, 
    name='up'
)
if __name__ == '__main__':
    command_line_interface(obj={})