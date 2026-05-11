import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import api from '../api/client'

export function useNotificationSummary(authIdentity, isAuthed, roles) {
  const notificationCount = ref(0)
  const hasPendingResourceReview = ref(false)
  const hasPendingDeleteRequest = ref(false)
  const notificationTimer = ref(null)
  const notificationFetchSeq = ref(0)
  const isPageVisible = ref(document.visibilityState === 'visible')

  const shouldPollNotifications = computed(() => isAuthed.value && isPageVisible.value)

  function resetNotificationState() {
    notificationCount.value = 0
    hasPendingResourceReview.value = false
    hasPendingDeleteRequest.value = false
  }

  function isResourceReviewNotification(item) {
    return item?.type === 'audit_pending' || /资源待审核|批量资源待审核/.test(`${item?.title || ''}${item?.content || ''}`)
  }

  function isDeleteRequestNotification(item) {
    return item?.type === 'delete_request' || /删除资源申请/.test(`${item?.title || ''}${item?.content || ''}`)
  }

  async function fetchNotificationSummary() {
    const currentFetchSeq = ++notificationFetchSeq.value
    if (!shouldPollNotifications.value) {
      resetNotificationState()
      return
    }

    try {
      const resp = await api.get('/api/notifications', { params: { page: 1, page_size: 100 } })
      if (currentFetchSeq !== notificationFetchSeq.value) return
      const items = resp.data.items || []
      notificationCount.value = items.filter((n) => !n.is_read).length
      hasPendingResourceReview.value = items.some((n) => !n.is_read && isResourceReviewNotification(n))
      hasPendingDeleteRequest.value = items.some((n) => !n.is_read && isDeleteRequestNotification(n))
    } catch {
      if (currentFetchSeq !== notificationFetchSeq.value) return
      resetNotificationState()
    }
  }

  function stopNotificationPolling() {
    if (notificationTimer.value) {
      window.clearInterval(notificationTimer.value)
      notificationTimer.value = null
    }
  }

  function startNotificationPolling() {
    stopNotificationPolling()
    if (!shouldPollNotifications.value) {
      resetNotificationState()
      return
    }
    fetchNotificationSummary()
    notificationTimer.value = window.setInterval(fetchNotificationSummary, 15000)
  }

  function handleVisibilityChange() {
    isPageVisible.value = document.visibilityState === 'visible'
  }

  function handleNotificationUpdated() {
    fetchNotificationSummary()
  }

  watch([authIdentity, shouldPollNotifications], () => {
    startNotificationPolling()
  })

  onMounted(() => {
    document.addEventListener('visibilitychange', handleVisibilityChange)
    window.addEventListener('notification-updated', handleNotificationUpdated)
    startNotificationPolling()
  })

  onBeforeUnmount(() => {
    document.removeEventListener('visibilitychange', handleVisibilityChange)
    window.removeEventListener('notification-updated', handleNotificationUpdated)
    stopNotificationPolling()
  })

  return {
    notificationCount,
    hasPendingResourceReview,
    hasPendingDeleteRequest,
    resetNotificationState,
    startNotificationPolling,
    stopNotificationPolling,
    fetchNotificationSummary,
  }
}
