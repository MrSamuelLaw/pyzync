#!./.venv/bin/python
"""Interactive CLI for restoring ZFS snapshots from remote backups."""

from typing import Optional

import dotenv

dotenv.load_dotenv()

from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt, Confirm

from pyzync.host import HostSnapshotManager as Host
from pyzync.storage.interfaces import RemoteSnapshotManager
from pyzync.interfaces import SnapshotGraph, SnapshotChain, SnapshotNode
from example_backup import dbx_adapter, file_adapter

console = Console()

# Adapter registry
ADAPTERS = {
    "1": {
        "name": "Dropbox",
        "adapter": dbx_adapter
    },
    "2": {
        "name": "Local File",
        "adapter": file_adapter
    },
}


def prompt_choice(prompt_text: str, num_choices: int) -> int:
    """Prompt the user to select from a range of numbered choices.

    Args:
        prompt_text: The prompt message to display to the user.
        num_choices: The number of choices available (1 to num_choices).

    Returns:
        Zero-based index of the selected choice.
    """
    choice = Prompt.ask(prompt_text, choices=[str(i) for i in range(1, num_choices + 1)])
    return int(choice) - 1


def select_adapter() -> tuple[str, RemoteSnapshotManager]:
    """Display available backup adapters and let the user select one.

    Returns:
        Tuple containing the adapter name and initialized RemoteSnapshotManager.
    """
    console.print("\n[bold]Available Backup Adapters:[/bold]")
    for key, config in ADAPTERS.items():
        console.print(f"  {key}. {config['name']}")

    choice = Prompt.ask("Select adapter", choices=list(ADAPTERS.keys()))
    adapter_config = ADAPTERS[choice]
    adapter_name = adapter_config["name"]
    manager = RemoteSnapshotManager(adapter=adapter_config["adapter"])

    console.print(f"[green]Selected: {adapter_name}[/green]")
    return adapter_name, manager


def display_chains(graphs: list[SnapshotGraph]) -> None:
    """Display all datasets and their snapshot chains in a formatted view.

    Args:
        graphs: List of snapshot graphs to display.
    """
    if not graphs:
        console.print("[yellow]No backup chains found.[/yellow]")
        return

    for graph in graphs:
        console.print(f"\n[bold]Dataset: {graph.dataset_id}[/bold]")
        chains = graph.get_chains()

        if not chains:
            console.print("  [yellow]No chains available[/yellow]")
            continue

        for chain_idx, chain in enumerate(chains, 1):
            console.print(f"  Chain {chain_idx}:")
            for node_idx, node in enumerate(chain, 1):
                node_type = "[cyan]complete[/cyan]" if node.node_type == "complete" else "[magenta]incremental[/magenta]"
                console.print(f"    {node_idx}. {node.dt} ({node_type})")


def select_dataset(graphs: list[SnapshotGraph]) -> Optional[SnapshotGraph]:
    """Prompt the user to select a dataset from available options.

    Automatically selects the only dataset if only one is available.

    Args:
        graphs: List of available snapshot graphs.

    Returns:
        Selected SnapshotGraph, or None if no datasets are available.
    """
    if not graphs:
        console.print("[red]No datasets available.[/red]")
        return None

    dataset_ids = [graph.dataset_id for graph in graphs]

    console.print("\n[bold]Available Datasets:[/bold]")
    for idx, dataset_id in enumerate(dataset_ids, 1):
        console.print(f"  {idx}. {dataset_id}")

    choice_idx = prompt_choice("Select dataset", len(dataset_ids))
    selected_graph = graphs[choice_idx]
    console.print(f"[green]Selected dataset: {selected_graph.dataset_id}[/green]")
    return selected_graph


def select_chain(graph: SnapshotGraph) -> Optional[SnapshotChain]:
    """Prompt the user to select a snapshot chain from a dataset.

     Automatically selects the only chain if only one is available.

     Args:
         graph: The snapshot graph containing available chains.

     Returns:
         Selected SnapshotChain, or None if no chains are available.
     """
    chains = graph.get_chains()

    if not chains:
        console.print("[red]No snapshot chains available for this dataset.[/red]")
        return None

    console.print("\n[bold]Available Chains:[/bold]")
    for chain_idx, chain in enumerate(chains, 1):
        console.print(f"  Chain {chain_idx}:")
        for node_idx, node in enumerate(chain, 1):
            node_type = "[cyan]complete[/cyan]" if node.node_type == "complete" else "[magenta]incremental[/magenta]"
            console.print(f"    {node_idx}. {node.dt} ({node_type})")

    choice_idx = prompt_choice("Select chain", len(chains))
    selected_chain = chains[choice_idx]
    console.print(f"[green]Selected chain with {len(selected_chain)} snapshots[/green]")
    return selected_chain


def select_restore_target(chain: SnapshotChain) -> Optional[SnapshotNode]:
    """Prompt the user to select a restore target snapshot from a chain.

    Args:
        chain: The snapshot chain containing available snapshots.

    Returns:
        Selected SnapshotNode to restore up to, or None if chain is empty.
    """
    console.print("\n[bold]Snapshots in Chain:[/bold]")
    for idx, node in enumerate(chain, 1):
        node_type = "[cyan]complete[/cyan]" if node.node_type == "complete" else "[magenta]incremental[/magenta]"
        console.print(f"  {idx}. {node.dt} ({node_type})")

    choice_idx = prompt_choice("Select snapshot to restore up to", len(chain))
    selected_node = chain[choice_idx]
    console.print(f"[green]Selected target: {selected_node.dt} ({selected_node.node_type})[/green]")
    return selected_node


def display_confirmation(adapter_name: str, dataset_id: str, chain: SnapshotChain,
                         target_node: SnapshotNode) -> bool:
    """Display a summary of the restore operation and request user confirmation.

    Args:
        adapter_name: Name of the selected backup adapter.
        dataset_id: ID of the dataset to restore.
        chain: The snapshot chain being restored.
        target_node: The specific snapshot to restore up to.

    Returns:
        True if user confirmed the restore operation, False otherwise.
    """
    table = Table(title="Restore Summary")
    table.add_column("Parameter", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Adapter", adapter_name)
    table.add_row("Dataset", dataset_id)
    table.add_row("Target Timestamp", str(target_node.dt))
    table.add_row("Target Type", target_node.node_type)
    table.add_row("Chain Length", str(len(chain)))

    console.print("\n")
    console.print(table)

    return Confirm.ask("\nProceed with restore?", default=False)


def execute_restore(manager: RemoteSnapshotManager, graph: SnapshotGraph, chain: SnapshotChain,
                    target_node: SnapshotNode) -> None:
    """Execute the restore operation by streaming data from remote backup.

    Restores the snapshot chain in order, handling both complete and incremental snapshots.

    Args:
        manager: The remote snapshot manager handling the backup connection.
        graph: The snapshot graph representing the dataset to restore.
        chain: The snapshot chain selected by the user.
        target_node: The specific snapshot to restore up to.

    Raises:
        Exception: If the restore operation fails.
    """
    console.print("\n[bold]Starting restore...[/bold]")

    try:
        # Find target_node's position in the chain
        target_index = chain.index(target_node)
        # Slice chain from start to target_node (inclusive)
        restore_chain = chain[:target_index + 1]

        # Get the local host graph for receiving data
        host_graph = Host.query(graph.dataset_id)[0]

        # Restore each snapshot in order
        for idx, node in enumerate(restore_chain, 1):
            console.print(
                f"[cyan]Restoring {idx} of {len(restore_chain)}: {node.dt} ({node.node_type})[/cyan]")

            try:
                # Get the producer from the remote adapter
                producer = manager.get_producer(node, graph)
                # Receive the data on the local host
                Host.recv(producer, host_graph, dryrun=False)
                console.print(f"[green]  ✓ Restored {node.dt}[/green]")

            except Exception as e:
                console.print(f"[red]  ✗ Failed to restore {node.dt}: {str(e)}[/red]")
                raise

        console.print("[green]Restore completed successfully![/green]")

    except Exception as e:
        console.print(f"[red]Restore failed: {str(e)}[/red]")
        raise


def main() -> None:
    """Main entry point for the restore CLI workflow.

    Orchestrates the entire restore process:
    1. Adapter selection
    2. Query available backup chains
    3. Dataset and chain selection
    4. Restore target selection
    5. Confirmation and execution
    """
    console.print("[bold blue]PyZync Restore CLI[/bold blue]")

    # Step 1: Adapter selection
    adapter_name, manager = select_adapter()

    # Step 2: Query backup chains
    console.print("\n[cyan]Querying backup chains...[/cyan]")
    graphs = manager.query(dataset_id=None)

    # Step 3: Dataset & chain selection
    graph = select_dataset(graphs)
    if graph is None:
        return

    chain = select_chain(graph)
    if chain is None:
        return

    # Step 4: Restore target selection
    target_node = select_restore_target(chain)
    if target_node is None:
        return

    # Step 5: Confirmation
    if not display_confirmation(adapter_name, str(graph.dataset_id), chain, target_node):
        console.print("[yellow]Restore cancelled.[/yellow]")
        return

    # Step 6: Execute restore
    execute_restore(manager, graph, chain, target_node)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user.[/yellow]")
    except Exception as e:
        console.print(f"\n[red]Error: {str(e)}[/red]")
        raise
