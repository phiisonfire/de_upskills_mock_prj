#!/usr/bin/env python3
"""CDK entry point for the CineInsight MovieLens AWS foundation."""

import aws_cdk as cdk

from stack import MovieLensFoundationStack


app = cdk.App()
MovieLensFoundationStack(
    app,
    "MovieLensFoundation",
    env=cdk.Environment(
        account=app.node.try_get_context("account") or None,
        region=app.node.try_get_context("region") or None,
    ),
)
app.synth()
