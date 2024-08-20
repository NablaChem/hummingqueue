import pyscfad.gto
import pyscfad.scf
import jax
import numpy as np


def alchemical_derivatives_RHF(
    atomspec: str, basis: str, includeonly: list[int], maxorder: int
):
    """Computes alchemical derivatives of the total RHF energy to arbitrary orders.

    In the return data structure, symmetry-equivalent values are not pruned, since
    their numerical value might differ. Differences between symmetry-equivalent values
    might indicate numerical instability.

    Parameters
    ----------
    atomspec : str
        PySCF-like specification of the unperturbed system.
    basis : str
        Basis set to run the calculation in.
    includeonly : list[int]
        Indices of the atoms of which to calculate the derivatives.
    maxorder : int
        Highest order of derivatives.

    Returns
    -------
    dict
        Keys: derivative order n. Values: n-dimensional tensor.
    """
    mol = pyscfad.gto.Mole()
    mol.atom = atomspec
    mol.basis = basis
    mol.symmetry = False
    mol.build(trace_exp=False, trace_ctr_coeff=False)

    mf = pyscfad.scf.RHF(mol)
    h1 = mf.get_hcore()

    def target(*Zs):
        # electronic: extend external potential
        s = 0
        for i, Z in zip(includeonly, Zs):
            mol.set_rinv_orig_(mol.atom_coords()[i])
            s -= Z * mol.intor("int1e_rinv")

        # nuclear: difference to already included NN repulsion
        nn = 0
        for i in range(mol.natm):
            Z_i = mol.atom_charge(i)
            if i in includeonly:
                Z_i += Zs[includeonly.index(i)]

            for j in range(i + 1, mol.natm):
                Z_j = mol.atom_charge(j)
                if j in includeonly:
                    Z_j += Zs[includeonly.index(j)]
                if i != j:
                    rij = np.linalg.norm(mol.atom_coords()[i] - mol.atom_coords()[j])
                    missing = Z_i * Z_j - mol.atom_charge(j) * mol.atom_charge(i)
                    nn += missing / rij

        mf.get_hcore = lambda *args, **kwargs: h1 + s
        return mf.kernel() + nn

    argnums = list(range(len(includeonly)))
    center = np.zeros(len(includeonly))

    e = mf.kernel()
    orders = dict({0: float(e)})

    last = target
    for order in range(1, maxorder + 1):
        print(f"Order {order}")
        if order == 1:
            # for first order, reverse mode is faster
            ad = jax.jacrev
        else:
            ad = jax.jacfwd
        last = ad(last, argnums=argnums)
        orders[order] = np.array(last(*center)).tolist()
    return orders


print(alchemical_derivatives_RHF("C 0 0 0; O 0 0 1", "sto-3g", [0, 1], 3))
