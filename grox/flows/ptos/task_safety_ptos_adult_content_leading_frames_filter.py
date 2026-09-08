import logging
from typing import override

from monitor.metrics import Metrics
from strato_http.queries.safety_post_annotations_result import (
    StratoSafetyPostAnnotationsResultDirectMh,
)

from grox.config.config import grox_config
from grox.core.data_loaders.data_types import Post, Video
from grox.core.schedules.types import TaskContext
from grox.core.tasks.task_filters import TaskFilterWithPost
from grox.flows.ptos.constants import (
    ADULT_CONTENT_LEADING_FRAMES_CROP_SECONDS,
    ADULT_CONTENT_LEADING_FRAMES_MIN_DURATION_SECONDS,
    ADULT_CONTENT_LEADING_FRAMES_UNCONDITIONAL_FAV,
)
from grox.flows.ptos.state import SafetyPolicyCategory

logger = logging.getLogger(__name__)

_METRIC_PREFIX = "task.safety_ptos_adult_content_leading_frames_filter"


class TaskSafetyPtosAdultContentLeadingFramesFilter(TaskFilterWithPost):
    _result_direct_mh = StratoSafetyPostAnnotationsResultDirectMh()

    @override
    @classmethod
    async def _eligible_with_post(cls, post: Post, ctx: TaskContext) -> bool:
        if not post.user:
            return cls._skip(post, "no_user")
        if (
            post.get_fav_count()
            < grox_config.media_hydration.deluxe_fav_count_threshold
        ):
            return cls._skip(post, "not_high_fav")
        media = [
            *(post.media or []),
            *(post.quoted_post.media or [] if post.quoted_post else []),
        ]
        long_videos = [
            m
            for m in media
            if isinstance(m, Video)
            and m.videoInfo
            and (m.videoInfo.durationMillis or 0)
            >= ADULT_CONTENT_LEADING_FRAMES_MIN_DURATION_SECONDS * 1000
        ]
        if not long_videos:
            return cls._skip(post, "no_long_video")
        prior = await cls._result_direct_mh.fetch(int(post.id))
        if prior is None:
            return cls._skip(post, "no_prior_ptos")
        if prior.safetyBoolMetadata and prior.safetyBoolMetadata.isNsfw:
            return cls._skip(post, "already_nsfw")
        prior_adult = any(
            v.category == SafetyPolicyCategory.AdultContent
            for a in prior.safetyPostAnnotations or []
            for v in a.violatedPolicies or []
        )
        if (
            not prior_adult
            and post.get_fav_count() < ADULT_CONTENT_LEADING_FRAMES_UNCONDITIONAL_FAV
        ):
            return cls._skip(post, "no_adult_1st_step")

        for video in long_videos:
            video.crop_seconds = ADULT_CONTENT_LEADING_FRAMES_CROP_SECONDS
        Metrics.counter(f"{_METRIC_PREFIX}.eligible.count").add(
            1, attributes={"prior_adult": str(prior_adult).lower()}
        )
        return True

    @classmethod
    def _skip(cls, post: Post, reason: str) -> bool:
        Metrics.counter(f"{_METRIC_PREFIX}.skipped.count").add(
            1, attributes={"reason": reason}
        )
        logger.info(f"Post {post.id}: skipped ({reason})")
        return False
