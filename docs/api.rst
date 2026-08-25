API Reference
=============

Everything ``hedloom`` exports, which is the whole authoring and running
surface: the study and flow decorators, the artifact and output declarations,
the placement policies, and the ``Site`` a plan is submitted to.

.. automodule:: hedloom
   :members:
   :imported-members:
   :undoc-members:
   :exclude-members: ArtifactContract, Parameter, Plan, Policy, ResourceContract

Re-exported plan types
----------------------

Part of what this package hands an author, but defined in ``hedloom-flow`` and
documented canonically on that unit's API page — repeated here because reading
them anywhere else would mean leaving the front door.

.. autoclass:: hedloom.ArtifactContract
   :members:
   :undoc-members:
   :no-index:

.. autoclass:: hedloom.Parameter
   :members:
   :undoc-members:
   :no-index:

.. autoclass:: hedloom.Plan
   :members:
   :undoc-members:
   :no-index:

.. autoclass:: hedloom.Policy
   :members:
   :undoc-members:
   :no-index:

.. autoclass:: hedloom.ResourceContract
   :members:
   :undoc-members:
   :no-index:
