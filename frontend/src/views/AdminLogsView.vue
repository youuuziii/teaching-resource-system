<script setup>
import { computed, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import api from '../api/client'

const loading = ref(false)
const items = ref([])
const pagination = ref({ page: 1, pageSize: 20, total: 0 })

const isAdmin = computed(() => {
  try {
    const u = JSON.parse(localStorage.getItem('user') || 'null')
    return Array.isArray(u?.roles) && u.roles.includes('admin')
  } catch {
    return false
  }
})

async function fetchLogs() {
  loading.value = true
  try {
    const resp = await api.get('/api/admin/logs', {
      params: {
        page: pagination.value.page,
        page_size: pagination.value.pageSize,
      },
    })
    items.value = resp.data.items || []
    pagination.value.total = resp.data.total ?? resp.data.items?.length ?? 0
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载失败')
  } finally {
    loading.value = false
  }
}

function handlePageChange(page) {
  pagination.value.page = page
  fetchLogs()
}

function handleSizeChange(size) {
  pagination.value.pageSize = size
  pagination.value.page = 1
  fetchLogs()
}

onMounted(fetchLogs)
</script>

<template>
  <el-card class="main-card logs-fixed" shadow="hover">
    <template #header>
      <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap">
        <div style="font-weight: 600">系统日志</div>
        <el-tag v-if="!isAdmin" type="warning">需要管理员权限</el-tag>
        <el-button type="primary" :loading="loading" @click="fetchLogs">刷新</el-button>
      </div>
    </template>

    <el-table :data="items" v-loading="loading" style="width: 100%">
      <el-table-column prop="created_at" label="时间" min-width="180" />
      <el-table-column prop="user_id" label="用户ID" width="100" />
      <el-table-column prop="method" label="方法" width="90" />
      <el-table-column prop="status_code" label="状态码" width="90" />
      <el-table-column prop="path" label="路径" min-width="240" />
      <el-table-column label="Meta" min-width="220">
        <template #default="{ row }">
          <el-text truncated>{{ JSON.stringify(row.meta || {}) }}</el-text>
        </template>
      </el-table-column>
    </el-table>
    <div class="table-pagination">
      <el-pagination
        v-model:current-page="pagination.page"
        v-model:page-size="pagination.pageSize"
        :total="pagination.total"
        :page-sizes="[20, 50, 100]"
        layout="总数, sizes, prev, pager, next, jumper"
        background
        @current-change="handlePageChange"
        @size-change="handleSizeChange"
      />
    </div>
  </el-card>
</template>
