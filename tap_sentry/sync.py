import os
import json
import asyncio
from pathlib import Path
from itertools import repeat
from urllib.parse import urljoin

import singer
import requests
import pendulum
from singer.bookmarks import write_bookmark, get_bookmark
from pendulum import datetime, period


class SentryAuthentication(requests.auth.AuthBase):
    def __init__(self, api_token: str):
        self.api_token = api_token

    def __call__(self, req):
        req.headers.update({"Authorization": " Bearer " + self.api_token})

        return req


class SentryClient:
    def __init__(self, auth: SentryAuthentication, url="https://sentry.io/api/0/"):
        self._base_url = url
        self._auth = auth
        self._session = None

    @property
    def session(self):
        if not self._session:
            self._session = requests.Session()
            self._session.auth = self._auth
            self._session.headers.update({"Accept": "application/json"})

        return self._session

    def _get(self, path, params=None):
        #url = urljoin(self._base_url, path)
        url = self._base_url + path
        response = self.session.get(url, params=params)
        response.raise_for_status()

        return response

    def bill(self, at: datetime):
        try:
            return self._get(f"billing/v2/year/{at.year}/month/{at.month}")
        except:
            return None

    def projects(self):
        try:
            projects = self._get(f"projects/")
            return projects.json()
        except:
            return None

    def issues(self, project_id, state):
        try:
            response = self._get(f"/organizations/split-software/issues/?project={project_id}&statsPeriod=1d")
            issues = response.json()
            url= response.url
            while (response.links['next']['results'] == 'true'):
                url = response.links['next']['url']
                response = self.session.get(url)
                issues += response.json()
            singer.write_bookmark(state, 'issues', 'latest', url)
            return issues

        except:
            return None

    def events(self, project_id, state):
        try:
            response = self._get(f"/organizations/split-software/events/?project={project_id}&statsPeriod=1d")
            events = response.json()
            url= response.url
            while (response.links['next']['results'] == 'true'):
                url = response.links['next']['url']
                response = self.session.get(url)
                events += response.json()
            singer.write_bookmark(state, 'issues', 'latest', url)
            return events
        except:
            return None

    def teams(self, state):
        try:
            response = self._get(f"/organizations/split-software/teams/")
            teams = response.json()
            extraction_time = singer.utils.now()
            while (response.links['next']['results'] == 'true'):
                url = response.links['next']['url']
                response = self.session.get(url)
                teams += response.json()
            singer.write_bookmark(state, 'teams', 'dateCreated', singer.utils.strftime(extraction_time))
            return teams
        except:
            return None

    def users(self, state):
        try:
            response = self._get(f"/organizations/split-software/users/")
            users = response.json()
            extraction_time = singer.utils.now()
            singer.write_bookmark(state, 'teams', 'dateCreated', singer.utils.strftime(extraction_time))
            return users
        except:
            return None


class SentrySync:
    def __init__(self, client: SentryClient, state={}):
        self._client = client
        self._state = state

    @property
    def client(self):
        return self._client

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        singer.write_state(value)
        self._state = value

    def sync(self, stream, schema):
        func = getattr(self, f"sync_{stream}")
        return func(schema)

    async def sync_issues(self, schema, period: pendulum.period = None):
        """Issues per project."""
        stream = "issues"
        loop = asyncio.get_running_loop()

        singer.write_schema(stream, schema.to_dict(), ["id"])
        projects = await loop.run_in_executor(None, self.client.projects)
        if projects:
            for project in projects:
                issues = await loop.run_in_executor(None, self.client.issues, project['id'], self.state)
                if (issues):
                    for issue in issues:
                        singer.write_record(stream, issue)


    async  def sync_events(self, schema, period: pendulum.period = None):
        """Events per project."""
        stream = "events"
        loop = asyncio.get_running_loop()

        singer.write_schema(stream, schema.to_dict(), ["eventID"])
        projects = await loop.run_in_executor(None, self.client.projects)
        if projects:
            for project in projects:
                events = await loop.run_in_executor(None, self.client.events, project['id'], self.state)
                if events:
                    for event in events:
                        singer.write_record(stream, event)

    async def sync_users(self, schema):
        "Users in the organization."
        stream = "users"
        loop = asyncio.get_running_loop()
        singer.write_schema(stream, schema.to_dict(), ["id"])
        users = await loop.run_in_executor(None, self.client.users, self.state)
        if users:
            for user in users:
                singer.write_record(stream, user)

    async def sync_teams(self, schema):
        "Teams in the organization."
        stream = "teams"
        loop = asyncio.get_running_loop()
        singer.write_schema(stream, schema.to_dict(), ["id"])
        teams = await loop.run_in_executor(None, self.client.teams, self.state)
        if teams:
            for team in teams:
                singer.write_record(stream, team)