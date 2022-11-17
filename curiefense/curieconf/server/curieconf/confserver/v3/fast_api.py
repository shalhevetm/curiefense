import datetime
import typing
from enum import Enum
from typing import Optional, List, Union

from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel, Field, validator
import random  # needed for generating a random number for an API
import uvicorn  # optional if you run it directly from terminal
import jsonschema

# monkey patch to force RestPlus to use Draft3 validator to benefit from "any" json type
jsonschema.Draft4Validator = jsonschema.Draft3Validator

# from curieconf import utils
# from curieconf.utils import cloud
# from curieconf.confserver import app


# TODO: TEMP DEFINITIONS
import os

app = FastAPI()
app.backend = Backends.get_backend(app, "git:///cf-persistent-config/confdb")
options = {}
val = os.environ.get("CURIECONF_TRUSTED_USERNAME_HEADER", None)
if val:
    options["trusted_username_header"] = val
val = os.environ.get("CURIECONF_TRUSTED_EMAIL_HEADER", None)
if val:
    options["trusted_email_header"] = val

import requests
from jsonschema import validate
from pathlib import Path
import json

# api_bp = Blueprint("api_v3", __name__)
# api = Api(api_bp, version="3.0", title="Curiefense configuration API server v3.0")

# ns_configs = api.namespace("configs", description="Configurations")
# ns_db = api.namespace("db", description="Database")
# ns_tools = api.namespace("tools", description="Tools")


##############
### MODELS ###
##############


### Models for documents
anyTypeUnion = Union[int, float, bool, object, list, None]
anyOp = Optional[object]
anyType = ["number", "string", "boolean", "object", "array", "null"]


# class AnyType(fields.Raw):
#     __schema_type__ = ["number", "string", "boolean", "object", "array", "null"]


# limit

class Threshold(BaseModel):
    limit: int
    action: str


# m_threshold = api.model(
#     "Rate Limit Threshold",
#     {
#         "limit": fields.Integer(required=True),
#         "action": fields.String(required=True),
#     },
# )

class Limit(BaseModel):
    id: str
    name: str
    description: Optional[str]
    _global: bool = Field(alias="global")
    active: bool
    timeframe: int
    thresholds: List[Threshold]
    include: typing.Any
    exclude: typing.Any
    key: anyTypeUnion
    pairwith: typing.Any
    tags: List[str]


# m_limit = api.model(
#     "Rate Limit",
#     {
#         "id": fields.String(required=True),
#         "name": fields.String(required=True),
#         "description": fields.String(),
#         "global": fields.Boolean(required=True),
#         "active": fields.Boolean(required=True),
#         "timeframe": fields.Integer(required=True),
#         "thresholds": fields.List(fields.Nested(m_threshold)),
#         "include": fields.Raw(required=True),
#         "exclude": fields.Raw(required=True),
#         "key": AnyType(required=True),
#         "pairwith": fields.Raw(required=True),
#         "tags": fields.List(fields.String()),
#     },
# )

# securitypolicy
class SecProfileMap(BaseModel):
    id: str
    name: str
    description: str
    match: str
    acl_profile: str
    acl_active: bool
    content_filter_profile: str
    content_filter_active: bool
    limit_ids: Optional[list]


# m_secprofilemap = api.model(
#     "Security Profile Map",
#     {
#         "id": fields.String(required=True),
#         "name": fields.String(required=True),
#         "description": fields.String(),
#         "match": fields.String(required=True),
#         "acl_profile": fields.String(required=True),
#         "acl_active": fields.Boolean(required=True),
#         "content_filter_profile": fields.String(required=True),
#         "content_filter_active": fields.Boolean(required=True),
#         "limit_ids": fields.List(fields.Raw()),
#     },
# )

# TODO  = deprecated?
# m_map = api.model(
#     "Security Profile Map", {"*": fields.Wildcard(fields.Nested(m_secprofilemap))}
# )

class SecurityPolicy(BaseModel):
    id: str
    name: str
    description: str
    tags: List[str]
    match: str
    session: anyTypeUnion
    session_ids: anyTypeUnion
    map: List[SecProfileMap]


# m_securitypolicy = api.model(
#     "Security Policy",
#     {
#         "id": fields.String(required=True),
#         "name": fields.String(required=True),
#         "description": fields.String(),
#         "tags": fields.List(fields.String()),
#         "match": fields.String(required=True),
#         "session": AnyType(),
#         "session_ids": AnyType(),
#         "map": fields.List(fields.Nested(m_secprofilemap)),
#     },
# )

# content filter rule

class ContentFilterRule(BaseModel):
    id: str
    name: str
    msg: str
    operand: str
    severity: int
    certainity: int
    category: str
    subcategory: str
    risk: int
    tags: Optional[List[str]]
    description: Optional[str]


# m_contentfilterrule = api.model(
#     "Content Filter Rule",
#     {
#         "id": fields.String(required=True),
#         "name": fields.String(required=True),
#         "msg": fields.String(required=True),
#         "operand": fields.String(required=True),
#         "severity": fields.Integer(required=True),
#         "certainity": fields.Integer(required=True),
#         "category": fields.String(required=True),
#         "subcategory": fields.String(required=True),
#         "risk": fields.Integer(required=True),
#         "tags": fields.List(fields.String()),
#         "description": fields.String(),
#     },
# )

# content filter profile
class ContentFilterProfile(BaseModel):
    id: str
    name: str
    description: Optional[str]
    ignore_alphanum: bool
    args: typing.Any
    headers: typing.Any
    cookies: typing.Any
    path: typing.Any
    allsections: typing.Any
    decoding: typing.Any
    masking_seed: str
    content_type: Optional[List[str]]
    active: Optional[List[str]]
    report: Optional[List[str]]
    ignore: Optional[List[str]]
    tags: Optional[List[str]]
    action: Optional[str]
    ignore_body: bool


# m_contentfilterprofile = api.model(
#     "Content Filter Profile",
#     {
#         "id": fields.String(required=True),
#         "name": fields.String(required=True),
#         "description": fields.String(),
#         "ignore_alphanum": fields.Boolean(required=True),
#         "args": fields.Raw(required=True),
#         "headers": fields.Raw(required=True),
#         "cookies": fields.Raw(required=True),
#         "path": fields.Raw(required=True),
#         "allsections": fields.Raw(),
#         "decoding": fields.Raw(required=True),
#         "masking_seed": fields.String(required=True),
#         "content_type": fields.List(fields.String()),
#         "active": fields.List(fields.String()),
#         "report": fields.List(fields.String()),
#         "ignore": fields.List(fields.String()),
#         "tags": fields.List(fields.String()),
#         "action": fields.String(),
#         "ignore_body": fields.Boolean(required=True),
#     },
# )

# aclprofile
class ACLProfile(BaseModel):
    id: str
    name: str
    description: Optional[str]
    allow: Optional[List[str]]
    allow_bot: Optional[List[str]]
    deny_bot: Optional[List[str]]
    passthrough: Optional[List[str]]
    deny: Optional[List[str]]
    force_deny: Optional[List[str]]
    tags: Optional[List[str]]
    action: Optional[str]


# m_aclprofile = api.model(
#     "ACL Profile",
#     {
#         "id": fields.String(required=True),
#         "name": fields.String(required=True),
#         "description": fields.String(),
#         "allow": fields.List(fields.String()),
#         "allow_bot": fields.List(fields.String()),
#         "deny_bot": fields.List(fields.String()),
#         "passthrough": fields.List(fields.String()),
#         "deny": fields.List(fields.String()),
#         "force_deny": fields.List(fields.String()),
#         "tags": fields.List(fields.String()),
#         "action": fields.String(),
#     },
# )

# Global Filter
class GlobalFilter(BaseModel):
    id: str
    name: str
    source: str
    mdate: str
    description: str
    active: bool
    action: typing.Any
    tags: Optional[List[str]]
    rule: anyTypeUnion


# m_glbalfilter = api.model(
#     "Global Filter",
#     {
#         "id": fields.String(required=True),
#         "name": fields.String(required=True),
#         "source": fields.String(required=True),
#         "mdate": fields.String(required=True),
#         "description": fields.String(),
#         "active": fields.Boolean(required=True),
#         "action": fields.Raw(required=True),
#         "tags": fields.List(fields.String()),
#         "rule": AnyType(),
#     },
# )

# Flow Control

class FlowControl(BaseModel):
    id: str
    name: str
    timeframe: int
    key: List[typing.Any]
    sequence: List[typing.Any]
    tags: Optional[List[str]]
    include: Optional[List[str]]
    exclude: Optional[List[str]]
    description: Optional[str]
    active: bool


#
# m_flowcontrol = api.model(
#     "Flow Control",
#     {
#         "id": fields.String(required=True),
#         "name": fields.String(required=True),
#         "timeframe": fields.Integer(required=True),
#         "key": fields.List(fields.Raw(required=True)),
#         "sequence": fields.List(fields.Raw(required=True)),
#         "tags": fields.List(fields.String()),
#         "include": fields.List(fields.String()),
#         "exclude": fields.List(fields.String()),
#         "description": fields.String(),
#         "active": fields.Boolean(required=True),
#     },
# )

# Action

class Action(BaseModel):
    id: str
    name: str
    description: Optional[str]
    tags: List[str]
    params: typing.Any
    type: str


# m_action = api.model(
#     "Action",
#     {
#         "id": fields.String(required=True),
#         "name": fields.String(required=True),
#         "description": fields.String(),
#         "tags": fields.List(fields.String(required=True)),
#         "params": fields.Raw(),
#         "type": fields.String(required=True),
#     },
# )

# Virtual Tag
class VirtualTag(BaseModel):
    id: str
    name: str
    description: Optional[str]
    match: List[typing.Any]


#
# m_virtualtag = api.model(
#     "Virtual Tag",
#     {
#         "id": fields.String(required=True),
#         "name": fields.String(required=True),
#         "description": fields.String(),
#         "match": fields.List(fields.Raw(required=True)),
#     },
# )

# custom
class Custom(BaseModel):
    id: str
    name: str


# m_custom = api.model(
#     "Custom",
#     {
#         "id": fields.String(required=True),
#         "name": fields.String(required=True),
#         "*": fields.Wildcard(fields.Raw()),
#     },
# )

### mapping from doc name to model

models = {
    "ratelimits": Limit,
    "securitypolicies": SecurityPolicy,
    "contentfilterrules": ContentFilterRule,
    "contentfilterprofiles": ContentFilterProfile,
    "aclprofiles": ACLProfile,
    "globalfilters": GlobalFilter,
    "flowcontrol": FlowControl,
    "actions": Action,
    "virtualtags": Custom,
    "custom": Custom,
}


### Other models
class DocumentMask(BaseModel):
    id: str
    name: str
    description: str
    map: Optional[List[SecProfileMap]]
    include: Optional[List[typing.Any]]
    exclude: Optional[List[typing.Any]]
    tags: Optional[List[str]]
    active: Optional[List[typing.Any]]
    action: typing.Any
    sequence: Optional[List[typing.Any]]
    timeframe: Optional[int]
    thresholds: Optional[List[Threshold]]
    pairwith: typing.Any
    content_type: Optional[List[str]]
    params: typing.Any
    decoding: typing.Any
    category: Optional[str]
    subcategory: Optional[str]
    risk: Optional[int]
    allow: Optional[List[str]]
    allow_bot: Optional[List[str]]
    deny_bot: Optional[List[str]]
    passthrough: Optional[List[str]]
    deny: Optional[List[str]]
    force_deny: Optional[List[str]]
    match: Optional[str] = "j"
    _type: Optional[str] = Field(alias="type")
    _star: Optional[List[typing.Any]] = Field(alias="*")


# m_document_mask = api.model(
#     "Mask for document",
#     {
#         "id": fields.String(required=True),
#         "name": fields.String(required=True),
#         "description": fields.String(required=True),
#         "map": fields.List(fields.Nested(m_secprofilemap)),
#         "include": fields.Wildcard(fields.Raw()),
#         "exclude": fields.Wildcard(fields.Raw()),
#         "tags": fields.List(fields.String()),
#         "active": fields.Wildcard(fields.Raw()),
#         "action": fields.Raw(),
#         "sequence": fields.List(fields.Raw()),
#         "timeframe": fields.Integer(),
#         "thresholds": fields.List(fields.Nested(m_threshold)),
#         "pairwith": fields.Raw(),
#         "content_type": fields.List(fields.String()),
#         "params": fields.Raw(),
#         "decoding": fields.Raw(),
#         "category": fields.String(),
#         "subcategory": fields.String(),
#         "risk": fields.Integer(),
#         "allow": fields.List(fields.String()),
#         "allow_bot": fields.List(fields.String()),
#         "deny_bot": fields.List(fields.String()),
#         "passthrough": fields.List(fields.String()),
#         "deny": fields.List(fields.String()),
#         "force_deny": fields.List(fields.String()),
#         "match": fields.String(),
#         "type": fields.String(),
#         "*": fields.Wildcard(fields.Raw()),
#     },
# )

class VersionLog(BaseModel):
    version: Optional[str]
    # TODO - dt_format="iso8601"
    date: Optional[datetime.datetime]
    _star: Optional[List[typing.Any]] = Field(alias="*")


#
# m_version_log = api.model(
#     "Version log",
#     {
#         "version": fields.String(),
#         "date": fields.DateTime(dt_format="iso8601"),
#         "*": fields.Wildcard(fields.Raw()),
#     },
# )

class Meta(BaseModel):
    id: str
    description: str
    date: Optional[datetime.datetime]
    logs: Optional[List[VersionLog]] = []
    version: Optional[str]


# m_meta = api.model(
#     "Meta",
#     {
#         "id": fields.String(required=True),
#         "description": fields.String(required=True),
#         "date": fields.DateTime(),
#         "logs": fields.List(fields.Nested(m_version_log), default=[]),
#         "version": fields.String(),
#     },
# )

class BlobEntry(BaseModel):
    format: str
    blob: anyTypeUnion


# m_blob_entry = api.model(
#     "Blob Entry",
#     {
#         "format": fields.String(required=True),
#         "blob": AnyType(),
#     },
# )

class BlobListEntry(BaseModel):
    name: Optional[str]


# m_blob_list_entry = api.model(
#     "Blob ListEntry",
#     {
#         "name": fields.String(),
#     },
# )

class DocumentListEntry(BaseModel):
    name: Optional[str]
    entries: Optional[int]


# m_document_list_entry = api.model(
#     "Document ListEntry",
#     {
#         "name": fields.String(),
#         "entries": fields.Integer(),
#     },
# )

class ConfigDocuments(BaseModel):
    ratelimits: Optional[List[models["ratelimits"]]] = []
    securitypolicies: Optional[List[models["securitypolicies"]]] = []
    contentfilterrules: Optional[List[models["contentfilterrules"]]] = []
    contentfilterprofiles: Optional[List[models["contentfilterprofiles"]]] = []
    aclprofiles: Optional[List[models["aclprofiles"]]] = []
    globalfilters: Optional[List[models["globalfilters"]]] = []
    flowcontrol: Optional[List[models["flowcontrol"]]] = []
    actions: Optional[List[models["actions"]]] = []
    virtualtags: Optional[List[models["virtualtags"]]] = []
    custom: Optional[List[models["custom"]]] = []


# m_config_documents = api.model(
#     "Config Documents",
#     {x: fields.List(fields.Nested(models[x], default=[])) for x in models},
# )


class ConfigBlobs(BaseModel):
    geolite2asn: Optional[List[Optional[BlobEntry]]]
    geolite2country: Optional[List[Optional[BlobEntry]]]
    geolite2city: Optional[List[Optional[BlobEntry]]]
    customconf: Optional[List[Optional[BlobEntry]]]


# m_config_blobs = api.model(
#     "Config Blobs",
#     {x: fields.Nested(m_blob_entry, default={}) for x in utils.BLOBS_PATH},
# )

class ConfigDeleteBlobs(BaseModel):
    geolite2asn: Optional[bool]
    geolite2country: Optional[bool]
    geolite2city: Optional[bool]
    customconf: Optional[bool]


# m_config_delete_blobs = api.model(
#     "Config Delete Blobs", {x: fields.Boolean() for x in utils.BLOBS_PATH}
# )

class Config(BaseModel):
    meta: Meta = {}
    documents: ConfigDocuments = {}
    blobs: ConfigBlobs = {}
    delete_documents: ConfigDocuments = {}
    delete_blobs: ConfigDeleteBlobs = {}


# m_config = api.model(
#     "Config",
#     {
#         "meta": fields.Nested(m_meta, default={}),
#         "documents": fields.Nested(m_config_documents, default={}),
#         "blobs": fields.Nested(m_config_blobs, default={}),
#         "delete_documents": fields.Nested(m_config_documents, default={}),
#         "delete_blobs": fields.Nested(m_config_delete_blobs, default={}),
#     },
# )

class Edit(BaseModel):
    path: str
    value: str


# m_edit = api.model(
#     "Edit",
#     {
#         "path": fields.String(required=True),
#         "value": fields.String(required=True),
#     },
# )

class BasicEntry(BaseModel):
    id: str
    name: str
    description: Optional[str]


# m_basic_entry = api.model(
#     "Basic Document Entry",
#     {
#         "id": fields.String(required=True),
#         "name": fields.String(required=True),
#         "description": fields.String(),
#     },
# )

### Publish

class Bucket(BaseModel):
    name: str
    url: str


# m_bucket = api.model(
#     "Bucket",
#     {
#         "name": fields.String(required=True),
#         "url": fields.String(required=True),
#     },
# )

### Git push & pull

class GitUrl(BaseModel):
    giturl: str


# m_giturl = api.model(
#     "GitUrl",
#     {
#         "giturl": fields.String(required=True),
#     },
# )

### Db
class DB(BaseModel):
    pass


# m_db = api.model("db", {})


### Document Schema validation


def validateJson(json_data, schema_type):
    try:
        validate(instance=json_data, schema=schema_type_map[schema_type])
    except jsonschema.exceptions.ValidationError as err:
        print(str(err))
        return False, str(err)
    return True, ""


### DB Schema validation


def validateDbJson(json_data, schema):
    try:
        validate(instance=json_data, schema=schema)
    except jsonschema.exceptions.ValidationError as err:
        print(str(err))
        return False
    return True


### Set git actor according to config & defined HTTP headers


def get_gitactor(request):
    email, username = "", ""
    email_header = app.options.get("trusted_email_header", None)
    if email_header:
        email = request.headers.get(email_header, "")
    username_header = app.options.get("trusted_username_header", None)
    if username_header:
        username = request.headers.get(username_header, "")
    return app.backend.prepare_actor(username, email)


base_path = Path(__file__).parent
# base_path = "/etc/curiefense/json/"
acl_profile_file_path = (base_path / "./json/acl-profile.schema").resolve()
with open(acl_profile_file_path) as json_file:
    acl_profile_schema = json.load(json_file)
ratelimits_file_path = (base_path / "./json/rate-limits.schema").resolve()
with open(ratelimits_file_path) as json_file:
    ratelimits_schema = json.load(json_file)
securitypolicies_file_path = (base_path / "./json/security-policies.schema").resolve()
with open(securitypolicies_file_path) as json_file:
    securitypolicies_schema = json.load(json_file)
content_filter_profile_file_path = (
        base_path / "./json/content-filter-profile.schema"
).resolve()
with open(content_filter_profile_file_path) as json_file:
    content_filter_profile_schema = json.load(json_file)
globalfilters_file_path = (base_path / "./json/global-filters.schema").resolve()
with open(globalfilters_file_path) as json_file:
    globalfilters_schema = json.load(json_file)
flowcontrol_file_path = (base_path / "./json/flow-control.schema").resolve()
with open(flowcontrol_file_path) as json_file:
    flowcontrol_schema = json.load(json_file)
content_filter_rule_file_path = (
        base_path / "./json/content-filter-rule.schema"
).resolve()
with open(content_filter_rule_file_path) as json_file:
    content_filter_rule_schema = json.load(json_file)
action_file_path = (base_path / "./json/action.schema").resolve()
with open(action_file_path) as json_file:
    action_schema = json.load(json_file)
virtualtag_file_path = (base_path / "./json/virtual-tags.schema").resolve()
with open(virtualtag_file_path) as json_file:
    virtual_tags_schema = json.load(json_file)
custom_file_path = (base_path / "./json/custom.schema").resolve()
with open(custom_file_path) as json_file:
    custom_schema = json.load(json_file)
schema_type_map = {
    "ratelimits": ratelimits_schema,
    "securitypolicies": securitypolicies_schema,
    "contentfilterprofiles": content_filter_profile_schema,
    "aclprofiles": acl_profile_schema,
    "globalfilters": globalfilters_schema,
    "flowcontrol": flowcontrol_schema,
    "contentfilterrules": content_filter_rule_schema,
    "actions": action_schema,
    "virtualtags": virtual_tags_schema,
    "custom": custom_schema,
}


class Tags(Enum):
    congifs = "configs"
    db = "db"
    tools = "tools"


################
### CONFIGS ###
################

@app.get("/configs/", tags=[Tags.congifs], response_model=Meta)
async def configs_get():
    """Get the detailed list of existing configurations"""
    return app.backend.configs_list()


@app.post("/configs/", tags=[Tags.congifs])
async def configs_post(config: Config, request: Request):
    """Create a new configuration"""
    data = dict(config)
    return app.backend.configs_create(data, get_gitactor(request))


#
# @ns_configs.route("/")
# class Configs(Resource):
#     # @ns_configs.marshal_list_with(m_meta, skip_none=True)
#     # def get(self):
#     #     "Get the detailed list of existing configurations"
#     #     return current_app.backend.configs_list()
#
#     @ns_configs.expect(m_config, validate=True)
#     def post(self):
#         "Create a new configuration"
#         data = request.json
#         return current_app.backend.configs_create(data, get_gitactor())

@app.get("/configs/{config}/", tags=[Tags.congifs], response_model=Config)
async def config_get(config: str):
    """Retrieve a complete configuration"""
    return app.backend.configs_get(config)


@app.post("/configs/{config}/", tags=[Tags.congifs])
async def config_post(config: str, m_config: Config, request: Request):
    "Create a new configuration. Configuration name in URL overrides configuration in POST data"
    data = dict(m_config)
    return app.backend.configs_create(data, config, get_gitactor(request))


@app.put("/configs/{config}/", tags=[Tags.congifs])
async def config_put(config: str, meta: Meta, request: Request):
    """Update an existing configuration"""
    data = dict(meta)
    return app.backend.configs_update(config, data, get_gitactor(request))


@app.delete("/configs/{config}/", tags=[Tags.congifs])
async def config_delete(config: str):
    """Delete a configuration"""
    return app.backend.configs_delete(config)


# @ns_configs.route("/<string:config>/")
# class Config(Resource):
#     # @ns_configs.marshal_with(m_config, skip_none=True)
#     # def get(self, config):
#     #     "Retrieve a complete configuration"
#     #     return current_app.backend.configs_get(config)
#
#     # @ns_configs.expect(m_config, validate=True)
#     # def post(self, config):
#     #     "Create a new configuration. Configuration name in URL overrides configuration in POST data"
#     #     data = request.json
#     #     return current_app.backend.configs_create(data, config, get_gitactor())
#
#     # @ns_configs.expect(m_meta, validate=True)
#     # def put(self, config):
#     #     "Update an existing configuration"
#     #     data = request.json
#     #     return current_app.backend.configs_update(config, data, get_gitactor())
#
#     def delete(self, config):
#         "Delete a configuration"
#         return current_app.backend.configs_delete(config)


@app.post("/configs/{config}/clone/", tags=[Tags.congifs])
async def config_clone_post(config: str, meta: Meta):
    """Clone a configuration. New name is provided in POST data"""
    data = dict(meta)
    return app.backend.configs_clone(config, data)


# @ns_configs.route("/<string:config>/clone/")
# class ConfigClone(Resource):
#     @ns_configs.expect(m_meta, validate=True)
#     def post(self, config):
#         "Clone a configuration. New name is provided in POST data"
#         data = request.json
#         return current_app.backend.configs_clone(config, data)
#

@app.post("/configs/{config}/clone/{new_name}/", tags=[Tags.congifs])
async def config_clone_name_post(config: str, new_name: str, meta: Meta):
    """Clone a configuration. New name is provided URL"""
    data = dict(meta)
    return app.backend.configs_clone(config, data, new_name)


# @ns_configs.route("/<string:config>/clone/<string:new_name>/")
# class ConfigCloneName(Resource):
#     @ns_configs.expect(m_meta, validate=True)
#     def post(self, config, new_name):
#         "Clone a configuration. New name is provided URL"
#         data = request.json
#         return current_app.backend.configs_clone(config, data, new_name)


@app.get("configs/{config}/v/", tags=[Tags.congifs], response_model=VersionLog)
async def config_list_version_get(config: str):
    """Get all versions of a given configuration"""
    return app.backend.configs_list_versions(config)


# @ns_configs.route("/<string:config>/v/")
# class ConfigListVersion(Resource):
#     @ns_configs.marshal_with(m_version_log, skip_none=True)
#     def get(self, config):
#         "Get all versions of a given configuration"
#         return current_app.backend.configs_list_versions(config)


@app.get("configs/{config}/v/{version}/", tags=[Tags.congifs])
async def config_version_get(config: str, version: str):
    """Retrieve a specific version of a configuration"""
    return app.backend.configs_get(config, version)


# @ns_configs.route("/<string:config>/v/<string:version>/")
# class ConfigVersion(Resource):
#     def get(self, config, version):
#         "Retrieve a specific version of a configuration"
#         return current_app.backend.configs_get(config, version)

@app.get("/{config}/v/{version}/revert/", tags=[Tags.congifs])
async def config_revert_put(config: str, version: str, request: Request):
    """Create a new version for a configuration from an old version"""
    return app.backend.configs_revert(config, version, get_gitactor(request))


# @ns_configs.route("/<string:config>/v/<string:version>/revert/")
# class ConfigRevert(Resource):
#     def put(self, config, version):
#         "Create a new version for a configuration from an old version"
#         return current_app.backend.configs_revert(config, version, get_gitactor())


#############
### Blobs ###
#############


@app.get("/configs/{config}/b/", tags=[Tags.congifs], response_model=BlobListEntry)
async def blobs_resource_get(config: str):
    """Retrieve the list of available blobs"""
    res = app.backend.blobs_list(config)
    return res


# @ns_configs.route("/<string:config>/b/")
# class BlobsResource(Resource):
#     @ns_configs.marshal_with(m_blob_list_entry, skip_none=True)
#     def get(self, config):
#         "Retrieve the list of available blobs"
#         res = current_app.backend.blobs_list(config)
#         return res

@app.get("configs/{config}/b/{blob}/", tags=[Tags.congifs], response_model=BlobEntry)
async def blob_resource_get(config: str, blob: str):
    """Retrieve a blob"""
    return app.backend.blobs_get(config, blob)


@app.post("configs/{config}/b/{blob}/", tags=[Tags.congifs])
async def blob_resource_post(config: str, blob: str, blob_entry: BlobEntry, request: Request):
    """Create a new blob"""
    return app.backend.blobs_create(
        config, blob, dict(blob_entry), get_gitactor(request)
    )


@app.put("configs/{config}/b/{blob}/", tags=[Tags.congifs])
async def blob_resource_put(config: str, blob: str, blob_entry: BlobEntry, request: Request):
    """Create a new blob"""
    return app.backend.blobs_update(
        config, blob, dict(blob_entry), get_gitactor(request)
    )


@app.delete("configs/{config}/b/{blob}/", tags=[Tags.congifs])
async def blob_resource_get(config: str, blob: str, request: Request):
    """Delete a blob"""
    return app.backend.blobs_delete(config, blob, get_gitactor(request))


#
# @ns_configs.route("/<string:config>/b/<string:blob>/")
# class BlobResource(Resource):
#     # @ns_configs.marshal_with(m_blob_entry, skip_none=True)
#     # def get(self, config, blob):
#     #     "Retrieve a blob"
#     #     return current_app.backend.blobs_get(config, blob)
#
#     # @ns_configs.expect(m_blob_entry, validate=True)
#     # def post(self, config, blob):
#     #     "Create a new blob"
#     #     return current_app.backend.blobs_create(
#     #         config, blob, request.json, get_gitactor()
#     #     )
#
#     # @ns_configs.expect(m_blob_entry, validate=True)
#     # def put(self, config, blob):
#     #     "Replace a blob with new data"
#     #     return current_app.backend.blobs_update(
#     #         config, blob, request.json, get_gitactor()
#     #     )
#
#     def delete(self, config, blob):
#         "Delete a blob"
#         return current_app.backend.blobs_delete(config, blob, get_gitactor())


@app.get("configs/{config}/b/{blob}/v/", tags=[Tags.congifs], response_model=VersionLog)
async def blob_list_version_resource_get(config: str, blob: str):
    "Retrieve the list of versions of a given blob"
    res = app.backend.blobs_list_versions(config, blob)
    return res


# @ns_configs.route("/<string:config>/b/<string:blob>/v/")
# class BlobListVersionResource(Resource):
#     @ns_configs.marshal_list_with(m_version_log, skip_none=True)
#     def get(self, config, blob):
#         "Retrieve the list of versions of a given blob"
#         res = current_app.backend.blobs_list_versions(config, blob)
#         return res


@app.get("configs/{config}/b/{blob}/v/{version}", tags=[Tags.congifs], response_model=VersionLog)
async def blob_version_resource_get(config: str, blob: str, version: str):
    """Retrieve the given version of a blob"""
    return app.backend.blobs_get(config, blob, version)


# @ns_configs.route("/<string:config>/b/<string:blob>/v/<string:version>/")
# class BlobVersionResource(Resource):
#     @ns_configs.marshal_list_with(m_version_log, skip_none=True)
#     def get(self, config, blob, version):
#         "Retrieve the given version of a blob"
#         return current_app.backend.blobs_get(config, blob, version)

@app.put("configs/{config}/b/{blob}/v/{version}/revert/", tags=[Tags.congifs])
async def blob_revert_resource_put(config: str, blob: str, version: str, request: Request):
    """Create a new version for a blob from an old version"""
    return app.backend.blobs_revert(config, blob, version, get_gitactor(request))


#
# @ns_configs.route("/<string:config>/b/<string:blob>/v/<string:version>/revert/")
# class BlobRevertResource(Resource):
#     def put(self, config, blob, version):
#         "Create a new version for a blob from an old version"
#         return current_app.backend.blobs_revert(config, blob, version, get_gitactor())


#################
### DOCUMENTS ###
#################

@app.get("/configs/{config}/d/", tags=[Tags.congifs], response_model=DocumentListEntry)
async def document_resource(config: str):
    """Retrieve the list of existing documents in this configuration"""
    res = app.backend.documents_list(config)
    return res


#
# @ns_configs.route("/<string:config>/d/")
# class DocumentsResource(Resource):
#     @ns_configs.marshal_with(m_document_list_entry, skip_none=True)
#     def get(self, config):
#         "Retrieve the list of existing documents in this configuration"
#         res = current_app.backend.documents_list(config)
#         return res


@app.get("/configs/{config}/d/{document}/", tags=[Tags.congifs], response_model=DocumentMask)
async def document_resource_get(config: str, document: str):
    """Get a complete document"""
    if document not in models:
        raise HTTPException(status_code=404, detail="document does not exist")
    res = app.backend.documents_get(config, document)
    res = {key: res[key] for key in list(models[document].__fields__.keys())}
    return res


async def _filter(data, keys):
    return {key: data[key] for key in keys}


@app.post("/configs/{config}/d/{document}/", tags=[Tags.congifs])
async def document_resource_post(config: str, document: str, basic_entries: List[BasicEntry], request: Request):
    """Create a new complete document"""
    if document not in models:
        raise HTTPException(status_code=404, detail="document does not exist")

    data = [_filter(dict(entry), list(models[document].__fields__.keys())) for entry in basic_entries]
    for entry in basic_entries:
        isValid, err = validateJson(dict(entry), document)
        if isValid is False:
            raise HTTPException(500, "schema mismatched: \n" + err)
    res = app.backend.documents_create(
        config, document, data, get_gitactor(request)
    )
    return res


@app.put("/configs/{config}/d/{document}/", tags=[Tags.congifs])
async def document_resource_put(config: str, document: str, basic_entries: List[BasicEntry], request: Request):
    """Update an existing document"""
    if document not in models:
        raise HTTPException(status_code=404, detail="document does not exist")

    data = [_filter(dict(entry), list(models[document].__fields__.keys())) for entry in basic_entries]
    for entry in basic_entries:
        isValid, err = validateJson(dict(entry), document)
        if isValid is False:
            raise HTTPException(500, "schema mismatched for entry: " + str(entry) + "\n" + err)
    res = app.backend.documents_update(
        config, document, data, get_gitactor(request)
    )
    return res


@app.delete("/configs/{config}/d/{document}/", tags=[Tags.congifs])
async def document_resource_delete(config: str, document: str, request: Request):
    """Delete/empty a document"""
    if document not in models:
        raise HTTPException(404, "document does not exist")
    res = app.backend.documents_delete(config, document, get_gitactor(request))
    return res


# @ns_configs.route("/<string:config>/d/<string:document>/")
# class DocumentResource(Resource):
#     # @ns_configs.marshal_with(m_document_mask, mask="*", skip_none=True)
#     # def get(self, config, document):
#     #     "Get a complete document"
#     #     if document not in models:
#     #         abort(404, "document does not exist")
#     #     res = current_app.backend.documents_get(config, document)
#     #     return marshal(res, models[document], skip_none=True)
#     #
#     # @ns_configs.expect([m_basic_entry], validate=True)
#     # def post(self, config, document):
#     #     "Create a new complete document"
#     #     if document not in models:
#     #         abort(404, "document does not exist")
#     #     data = marshal(request.json, models[document], skip_none=True)
#     #     for entry in request.json:
#     #         isValid, err = validateJson(entry, document)
#     #         if isValid is False:
#     #             abort(500, "schema mismatched: \n" + err)
#     #     res = current_app.backend.documents_create(
#     #         config, document, data, get_gitactor()
#     #     )
#     #     return res
#
#     # @ns_configs.expect([m_basic_entry], validate=True)
#     # def put(self, config, document):
#     #     "Update an existing document"
#     #     if document not in models:
#     #         abort(404, "document does not exist")
#     #     data = marshal(request.json, models[document], skip_none=True)
#     #     for entry in request.json:
#     #         isValid, err = validateJson(entry, document)
#     #         if isValid is False:
#     #             abort(500, "schema mismatched for entry: " + str(entry) + "\n" + err)
#     #     res = current_app.backend.documents_update(
#     #         config, document, data, get_gitactor()
#     #     )
#     #     return res
#
#     # def delete(self, config, document):
#     #     "Delete/empty a document"
#     #     if document not in models:
#     #         abort(404, "document does not exist")
#     #     res = current_app.backend.documents_delete(config, document, get_gitactor())
#     #     return res


@app.get("/configs/{config}/d/{document}/v/", tags=[Tags.congifs])
async def document_list_version_resource_get(config: str, document: str):
    """Retrieve the existing versions of a given document"""
    if document not in models:
        raise HTTPException(404, "document does not exist")
    res = app.backend.documents_list_versions(config, document)
    res = {key: res[key] for key in list(VersionLog.__fields__.keys())}
    return res


#
# @ns_configs.route("/<string:config>/d/<string:document>/v/")
# class DocumentListVersionResource(Resource):
#     def get(self, config, document):
#         "Retrieve the existing versions of a given document"
#         if document not in models:
#             abort(404, "document does not exist")
#         res = current_app.backend.documents_list_versions(config, document)
#         return marshal(res, m_version_log, skip_none=True)


@app.get("/configs/{config}/d/{document}/v/{version}/", tags=[Tags.congifs])
async def document_version_resource_get(config: str, document: str, version: str):
    """Get a given version of a document"""
    if document not in models:
        raise HTTPException(404, "document does not exist")
    res = app.backend.documents_get(config, document, version)
    return {key: res[key] for key in list(models[document].__fields__.keys())}


# @ns_configs.route("/<string:config>/d/<string:document>/v/<string:version>/")
# class DocumentVersionResource(Resource):
#     def get(self, config, document, version):
#         "Get a given version of a document"
#         if document not in models:
#             abort(404, "document does not exist")
#         res = current_app.backend.documents_get(config, document, version)
#         return marshal(res, models[document], skip_none=True)

@app.put("/configs/{config}/d/{document}/v/{version}/revert/", tags=[Tags.congifs])
async def document_revert_resource_put(config: str, document: str, version: str, request: Request):
    """Create a new version for a document from an old version"""
    return app.backend.documents_revert(
        config, document, version, get_gitactor(request)
    )


# @ns_configs.route("/<string:config>/d/<string:document>/v/<string:version>/revert/")
# class DocumentRevertResource(Resource):
#     def put(self, config, document, version):
#         "Create a new version for a document from an old version"
#         return current_app.backend.documents_revert(
#             config, document, version, get_gitactor()
#         )


###############
### ENTRIES ###
###############

@app.get("/configs/{config}/d/{document}/e/", tags=[Tags.congifs])
async def entries_resource_get(config: str, document: str):
    """Retrieve the list of entries in a document"""
    if document not in models:
        raise HTTPException(404, "document does not exist")
    res = app.backend.entries_list(config, document)
    return res  # XXX: marshal


@app.post("/configs/{config}/d/{document}/e/", tags=[Tags.congifs])
async def entries_resource_post(config: str, document: str, basic_entry: BasicEntry, request: Request):
    "Create an entry in a document"
    if document not in models:
        raise HTTPException(404, "document does not exist")
    isValid, err = validateJson(dict(basic_entry), document)
    if isValid:
        data = {key: dict(basic_entry)[key] for key in list(models[document].__fields__.keys())}
        res = app.backend.entries_create(
            config, document, data, get_gitactor(request)
        )
        return res
    else:
        raise HTTPException(500, "schema mismatched: \n" + err)


# @ns_configs.route("/<string:config>/d/<string:document>/e/")
# class EntriesResource(Resource):
#     # def get(self, config, document):
#     #     "Retrieve the list of entries in a document"
#     #     if document not in models:
#     #         abort(404, "document does not exist")
#     #     res = current_app.backend.entries_list(config, document)
#     #     return res  # XXX: marshal
#
#     @ns_configs.expect(m_basic_entry, validate=True)
#     def post(self, config, document):
#         "Create an entry in a document"
#         if document not in models:
#             abort(404, "document does not exist")
#         isValid, err = validateJson(request.json, document)
#         if isValid:
#             data = marshal(request.json, models[document], skip_none=True)
#             res = current_app.backend.entries_create(
#                 config, document, data, get_gitactor()
#             )
#             return res
#         else:
#             abort(500, "schema mismatched: \n" + err)


@app.get("/configs/{config}/d/{document}/e/{entry}/", tags=[Tags.congifs])
async def entry_resource_get(config: str, document: str, entry: str):
    """Retrieve an entry from a document"""
    if document not in models:
        raise HTTPException(404, "document does not exist")
    res = app.backend.entries_get(config, document, entry)
    return {key: res for key in list(models[document].__fields__.keys())}


@app.put("/configs/{config}/d/{document}/e/{entry}/", tags=[Tags.congifs])
async def entry_resource_put(config: str, document: str, entry: str, basic_entry: BasicEntry, request: Request):
    """Update an entry in a document"""
    if document not in models:
        raise HTTPException(404, "document does not exist")
    isValid, err = validateJson(dict(basic_entry), document)
    if isValid:
        data = {key: dict(basic_entry)[key] for key in list(models[document].__fields__.keys())}

        res = app.backend.entries_update(
            config, document, entry, data, get_gitactor(request)
        )
        return res
    else:
        raise HTTPException(500, "schema mismatched: \n" + err)


@app.delete("/configs/{config}/d/{document}/e/{entry}/", tags=[Tags.congifs])
async def entry_resource_deleye(config: str, document: str, entry: str, request: Request):
    """Delete an entry from a document"""
    if document not in models:
        raise HTTPException(404, "document does not exist")
    res = app.backend.entries_delete(
        config, document, entry, get_gitactor(request)
    )
    return res


# @ns_configs.route("/<string:config>/d/<string:document>/e/<string:entry>/")
# class EntryResource(Resource):
#     # def get(self, config, document, entry):
#     #     "Retrieve an entry from a document"
#     #     if document not in models:
#     #         abort(404, "document does not exist")
#     #     res = current_app.backend.entries_get(config, document, entry)
#     #     return marshal(res, models[document], skip_none=True)
#
#     # @ns_configs.expect(m_basic_entry, validate=True)
#     # def put(self, config, document, entry):
#     #     "Update an entry in a document"
#     #     if document not in models:
#     #         abort(404, "document does not exist")
#     #     isValid, err = validateJson(request.json, document)
#     #     if isValid:
#     #         data = marshal(request.json, models[document], skip_none=True)
#     #         res = current_app.backend.entries_update(
#     #             config, document, entry, data, get_gitactor()
#     #         )
#     #         return res
#     #     else:
#     #         abort(500, "schema mismatched: \n" + err)
#
#     # def delete(self, config, document, entry):
#     #     "Delete an entry from a document"
#     #     if document not in models:
#     #         abort(404, "document does not exist")
#     #     res = current_app.backend.entries_delete(
#     #         config, document, entry, get_gitactor()
#     #     )
#     #     return res


@app.get("/configs/{config}/d/{document}/e/{entry}/v/", tags=[Tags.congifs])
async def entry_list_version_resource_get(config: str, document: str, entry: str):
    """Get the list of existing versions of a given entry in a document"""
    if document not in models:
        raise HTTPException(404, "document does not exist")
    res = app.backend.entries_list_versions(config, document, entry)
    return {key: res[key] for key in list(VersionLog.__fields__.keys())}


#
# @ns_configs.route("/<string:config>/d/<string:document>/e/<string:entry>/v/")
# class EntryListVersionResource(Resource):
#     def get(self, config, document, entry):
#         "Get the list of existing versions of a given entry in a document"
#         if document not in models:
#             abort(404, "document does not exist")
#         res = current_app.backend.entries_list_versions(config, document, entry)
#         return marshal(res, m_version_log, skip_none=True)


@app.get("/configs/{config}/d/{document}/e/{entry}/v/{version}/", tags=[Tags.congifs])
async def entry_version_resource_get(config: str, document: str, entry: str, version: str):
    """Get a given version of a document entry"""
    if document not in models:
        raise HTTPException(404, "document does not exist")
    res = app.backend.entries_get(config, document, entry, version)
    return {key: res[key] for key in list(models[document].__fields__.keys())}


# @ns_configs.route(
#     "/<string:config>/d/<string:document>/e/<string:entry>/v/<string:version>/"
# )
# class EntryVersionResource(Resource):
#     def get(self, config, document, entry, version):
#         "Get a given version of a document entry"
#         if document not in models:
#             abort(404, "document does not exist")
#         res = current_app.backend.entries_get(config, document, entry, version)
#         return marshal(res, models[document], skip_none=True)


################
### Database ###
################


@ns_db.route("/")
class DbResource(Resource):
    def get(self):
        "Get the list of existing namespaces"
        return current_app.backend.ns_list()


@ns_db.route("/v/")
class DbQueryResource(Resource):
    def get(self):
        "List all existing versions of namespaces"
        return current_app.backend.ns_list_versions()


@ns_db.route("/<string:nsname>/")
class NSResource(Resource):
    def get(self, nsname):
        "Get a complete namespace"
        try:
            return current_app.backend.ns_get(nsname, version=None)
        except KeyError:
            abort(404, "namespace [%s] does not exist" % nsname)

    @ns_db.expect(m_db, validate=True)
    def post(self, nsname):
        "Create a non-existing namespace from data"
        try:
            return current_app.backend.ns_create(nsname, request.json, get_gitactor())
        except Exception:
            abort(409, "namespace [%s] already exists" % nsname)

    @ns_db.expect(m_db, validate=True)
    def put(self, nsname):
        "Merge data into a namespace"
        return current_app.backend.ns_update(nsname, request.json, get_gitactor())

    def delete(self, nsname):
        "Delete an existing namespace"
        try:
            return current_app.backend.ns_delete(nsname, get_gitactor())
        except KeyError:
            abort(409, "namespace [%s] does not exist" % nsname)


@ns_db.route("/<string:nsname>/v/<string:version>/")
class NSVersionResource(Resource):
    def get(self, nsname, version):
        "Get a given version of a namespace"
        return current_app.backend.ns_get(nsname, version)


@ns_db.route("/<string:nsname>/v/<string:version>/revert/")
class NSVersionResource(Resource):
    def put(self, nsname, version):
        "Create a new version for a namespace from an old version"
        try:
            return current_app.backend.ns_revert(nsname, version, get_gitactor())
        except KeyError:
            abort(404, "namespace [%s] version [%s] not found" % (nsname, version))


@ns_db.route("/<string:nsname>/q/")
class NSQueryResource(Resource):
    def post(self, nsname):
        "Run a JSON query on the namespace and returns the results"
        return current_app.backend.ns_query(nsname, request.json)


@ns_db.route("/<string:nsname>/k/")
class KeysResource(Resource):
    def get(self, nsname):
        "List all keys of a given namespace"
        return current_app.backend.key_list(nsname)


@ns_db.route("/<string:nsname>/k/<string:key>/v/")
class KeysListVersionsResource(Resource):
    def get(self, nsname, key):
        "Get all versions of a given key in namespace"
        return current_app.backend.key_list_versions(nsname, key)


@ns_db.route("/<string:nsname>/k/<string:key>/")
class KeyResource(Resource):
    def get(self, nsname, key):
        "Retrieve a given key's value from a given namespace"
        return current_app.backend.key_get(nsname, key)

    def put(self, nsname, key):
        "Create or update the value of a key"
        # check if "reblaze/k/<key>" exists in system/schema-validation
        if nsname != "system":
            keyName = nsname + "/k/" + key
            schemas = current_app.backend.key_get("system", "schema-validation")
            schema = None
            # find schema if exists and validate the json input
            for item in schemas.items():
                if item[0] == keyName:
                    schema = item[1]
                    break
            if schema:
                isValid = validateDbJson(request.json, schema)
                if isValid is False:
                    abort(500, "schema mismatched")
        return current_app.backend.key_set(nsname, key, request.json, get_gitactor())

    def delete(self, nsname, key):
        "Delete a key"
        return current_app.backend.key_delete(nsname, key, get_gitactor())


#############
### Tools ###
#############


req_fetch_parser = reqparse.RequestParser()
req_fetch_parser.add_argument("url", location="args", help="url to retrieve")


@ns_tools.route("/fetch")
class FetchResource(Resource):
    @ns_tools.expect(req_fetch_parser, validate=True)
    def get(self):
        "Fetch an URL"
        args = req_fetch_parser.parse_args()
        try:
            r = requests.get(args.url)
        except Exception as e:
            abort(400, "cannot retrieve [%s]: %s" % (args.url, e))
        return make_response(r.content)


@ns_tools.route("/publish/<string:config>/")
@ns_tools.route("/publish/<string:config>/v/<string:version>/")
class PublishResource(Resource):
    @ns_tools.expect([m_bucket], validate=True)
    def put(self, config, version=None):
        "Push configuration to s3 buckets"
        conf = current_app.backend.configs_get(config, version)
        ok = True
        status = []
        if type(request.json) is not list:
            abort(400, "body must be a list")
        for bucket in request.json:
            logs = []
            try:
                cloud.export(conf, bucket["url"], prnt=lambda x: logs.append(x))
            except Exception as e:
                ok = False
                s = False
                msg = repr(e)
            else:
                s = True
                msg = "ok"
            status.append(
                {"name": bucket["name"], "ok": s, "logs": logs, "message": msg}
            )
        return make_response({"ok": ok, "status": status})


@ns_tools.route("/gitpush/")
class GitPushResource(Resource):
    @ns_tools.expect([m_giturl], validate=True)
    def put(self):
        "Push git configuration to remote git repositories"
        ok = True
        status = []
        for giturl in request.json:
            try:
                current_app.backend.gitpush(giturl["giturl"])
            except Exception as e:
                msg = repr(e)
                s = False
            else:
                msg = "ok"
                s = True
            status.append({"url": giturl["giturl"], "ok": s, "message": msg})
        return make_response({"ok": ok, "status": status})


@ns_tools.route("/gitfetch/")
class GitFetchResource(Resource):
    @ns_tools.expect(m_giturl, validate=True)
    def put(self):
        "Fetch git configuration from specified remote repository"
        ok = True
        try:
            current_app.backend.gitfetch(request.json["giturl"])
        except Exception as e:
            ok = False
            msg = repr(e)
        else:
            msg = "ok"
        return make_response({"ok": ok, "status": msg})


if __name__ == '__main__':
    print("hi")