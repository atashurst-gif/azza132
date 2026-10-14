"""Promoted lab ideas on the IC Markets DEMO account (Aaron, 14 Oct 2026).

* ``roster.py``  - which ideas trade, their magic numbers and risk tiers (written by the weekly pick);
* ``record.py``  - the permanent record of every demo trade, from the broker's deals;
* ``engine.py``  - the engine inside the trader: the lab's own practice rules, genuine orders, the stop at
  the broker, DEMO accounts only.

This package sits OUTSIDE ``mintel/lab`` on purpose: the lab itself still cannot place an order
(tests/test_lab_safety.py), and its practice process keeps every idea on paper beside the demo.
"""
