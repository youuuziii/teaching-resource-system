<script setup>
import { computed, onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import api from '../api/client'

const tab = ref('users')

const usersLoading = ref(false)
const users = ref([])
const userQuery = ref('')
const roleFilter = ref('all')

const rolesOfMe = computed(() => {
  try {
    const u = JSON.parse(localStorage.getItem('user') || 'null')
    return Array.isArray(u?.roles) ? u.roles : []
  } catch {
    return []
  }
})
const isSystemAdmin = computed(() => rolesOfMe.value.includes('admin'))
const isDean = computed(() => rolesOfMe.value.includes('dean') && !rolesOfMe.value.includes('admin'))

const roleFilterOptions = computed(() => {
  const base = [
    { label: '全部', value: 'all' },
    { label: '教师', value: 'teacher' },
    { label: '学生', value: 'student' },
  ]
  if (isSystemAdmin.value) base.splice(1, 0, { label: '教务管理员', value: 'dean' })
  if (isSystemAdmin.value) base.splice(1, 0, { label: '管理员', value: 'admin' })
  return base
})

const rbacLoading = ref(false)
const roles = ref([])
const permissions = ref([])

const roleOptions = computed(() => (roles.value || []).map((r) => ({ label: r.name, value: r.name })))
const allowedRoleOptions = computed(() => {
  if (isSystemAdmin.value) return roleOptions.value
  return roleOptions.value.filter((r) => r.value === 'teacher' || r.value === 'student')
})
const permissionOptions = computed(() => (permissions.value || []).map((p) => ({ label: p.code, value: p.code })))

async function loadUsers() {
  usersLoading.value = true
  try {
    const role = roleFilter.value && roleFilter.value !== 'all' ? roleFilter.value : undefined
    const resp = await api.get('/api/admin/users', { params: { q: userQuery.value || undefined, role } })
    users.value = resp.data.items || []
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载用户失败')
  } finally {
    usersLoading.value = false
  }
}

async function loadRbac() {
  rbacLoading.value = true
  try {
    const resp = await api.get('/api/admin/rbac')
    roles.value = resp.data.roles || []
    permissions.value = resp.data.permissions || []
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载RBAC失败')
  } finally {
    rbacLoading.value = false
  }
}

const createOpen = ref(false)
const createForm = ref({ username: '', password: '', roles: ['teacher'], is_active: true })

function openCreate() {
  createForm.value = { username: '', password: '', roles: ['teacher'], is_active: true }
  createOpen.value = true
}

async function submitCreate() {
  const username = (createForm.value.username || '').trim()
  const password = (createForm.value.password || '').trim()
  if (!username || !password) {
    ElMessage.warning('请输入用户名和密码')
    return
  }
  try {
    await api.post('/api/admin/users', {
      username,
      password,
      roles: createForm.value.roles || [],
      is_active: !!createForm.value.is_active,
    })
    ElMessage.success('创建成功')
    createOpen.value = false
    await loadUsers()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '创建失败')
  }
}

const editOpen = ref(false)
const editForm = ref({ id: null, username: '', roles: [], is_active: true, password: '' })

function openEdit(row) {
  editForm.value = {
    id: row.id,
    username: row.username,
    roles: Array.isArray(row.roles) ? [...row.roles] : [],
    is_active: !!row.is_active,
    password: '',
  }
  editOpen.value = true
}

async function submitEdit() {
  const id = editForm.value.id
  if (!id) return
  const payload = {
    roles: editForm.value.roles || [],
    is_active: !!editForm.value.is_active,
  }
  const pwd = (editForm.value.password || '').trim()
  if (pwd) payload.password = pwd
  try {
    await api.patch(`/api/admin/users/${id}`, payload)
    ElMessage.success('保存成功')
    editOpen.value = false
    await loadUsers()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '保存失败')
  }
}

async function removeUser(row) {
  try {
    await ElMessageBox.confirm(`确认删除账号「${row.username}」？`, '删除确认', { type: 'warning' })
  } catch {
    return
  }
  try {
    await api.delete(`/api/admin/users/${row.id}`)
    ElMessage.success('删除成功')
    await loadUsers()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '删除失败')
  }
}

const roleEditOpen = ref(false)
const roleEditForm = ref({ id: null, name: '', permission_codes: [] })
const newPermCode = ref('')

function openRoleEdit(row) {
  roleEditForm.value = {
    id: row.id,
    name: row.name,
    permission_codes: Array.isArray(row.permissions) ? [...row.permissions] : [],
  }
  roleEditOpen.value = true
}

async function submitRolePerms() {
  const id = roleEditForm.value.id
  if (!id) return
  try {
    await api.put(`/api/admin/roles/${id}/permissions`, { permission_codes: roleEditForm.value.permission_codes || [] })
    ElMessage.success('保存成功')
    roleEditOpen.value = false
    await loadRbac()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '保存失败')
  }
}

async function createPermission() {
  const code = (newPermCode.value || '').trim()
  if (!code) return
  try {
    await api.post('/api/admin/permissions', { code })
    newPermCode.value = ''
    await loadRbac()
    ElMessage.success('新增权限码成功')
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '新增权限码失败')
  }
}

onMounted(async () => {
  if (isSystemAdmin.value) {
    await loadRbac()
  } else {
    roles.value = [{ id: -1, name: 'teacher', permissions: [] }, { id: -2, name: 'student', permissions: [] }]
    permissions.value = []
  }
  await loadUsers()
})
</script>

<template>
  <el-card>
    <template #header>
      <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap">
        <div style="font-weight: 600">账号与权限管理</div>
        <el-button :loading="usersLoading || rbacLoading" @click="() => Promise.all([isSystemAdmin ? loadRbac() : Promise.resolve(), loadUsers()])">刷新</el-button>
      </div>
    </template>

    <el-tabs v-model="tab">
      <el-tab-pane label="账号管理" name="users">
        <div style="display: flex; gap: 10px; align-items: center; flex-wrap: wrap; margin-bottom: 12px">
          <el-select v-model="roleFilter" placeholder="角色筛选" style="width: 140px" @change="loadUsers">
            <el-option v-for="opt in roleFilterOptions" :key="opt.value" :label="opt.label" :value="opt.value" />
          </el-select>
          <el-input v-model="userQuery" placeholder="按用户名搜索" style="width: 240px" clearable @keyup.enter="loadUsers" />
          <el-button :loading="usersLoading" @click="loadUsers">搜索</el-button>
          <el-button type="primary" @click="openCreate">新增账号</el-button>
        </div>

        <el-table :data="users" v-loading="usersLoading" style="width: 100%">
          <el-table-column prop="username" label="用户名" min-width="160" />
          <el-table-column label="角色" min-width="220">
            <template #default="{ row }">
              <el-tag v-for="r in row.roles || []" :key="r" style="margin-right: 6px" size="small">{{ r }}</el-tag>
              <span v-if="(row.roles || []).length === 0">-</span>
            </template>
          </el-table-column>
          <el-table-column label="状态" width="120">
            <template #default="{ row }">
              <el-tag :type="row.is_active ? 'success' : 'info'">{{ row.is_active ? '启用' : '禁用' }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="created_at" label="创建时间" min-width="200" />
          <el-table-column label="操作" width="180">
            <template #default="{ row }">
              <el-button size="small" @click="openEdit(row)">编辑</el-button>
              <el-button size="small" type="danger" @click="removeUser(row)">删除</el-button>
            </template>
          </el-table-column>
        </el-table>
      </el-tab-pane>

      <el-tab-pane v-if="isSystemAdmin" label="角色权限" name="rbac">
        <div style="display: flex; gap: 10px; align-items: center; flex-wrap: wrap; margin-bottom: 12px">
          <el-input v-model="newPermCode" placeholder="新增权限码（如 admin.users.manage）" style="width: 320px" clearable />
          <el-button :loading="rbacLoading" type="primary" @click="createPermission">新增权限码</el-button>
        </div>

        <el-table :data="roles" v-loading="rbacLoading" style="width: 100%">
          <el-table-column prop="name" label="角色" width="160" />
          <el-table-column label="权限码" min-width="420">
            <template #default="{ row }">
              <el-tag v-for="p in row.permissions || []" :key="p" style="margin-right: 6px; margin-bottom: 6px" size="small">
                {{ p }}
              </el-tag>
              <span v-if="(row.permissions || []).length === 0">-</span>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="140">
            <template #default="{ row }">
              <el-button size="small" @click="openRoleEdit(row)">编辑权限</el-button>
            </template>
          </el-table-column>
        </el-table>
      </el-tab-pane>
    </el-tabs>
  </el-card>

  <el-dialog v-model="createOpen" title="新增账号" width="520px">
    <el-form :model="createForm" label-width="90px">
      <el-form-item label="用户名">
        <el-input v-model="createForm.username" placeholder="如 teacher001" />
      </el-form-item>
      <el-form-item label="密码">
        <el-input v-model="createForm.password" placeholder="初始密码" show-password />
      </el-form-item>
      <el-form-item label="角色">
        <el-select v-model="createForm.roles" multiple filterable style="width: 100%" placeholder="选择角色">
          <el-option v-for="opt in allowedRoleOptions" :key="opt.value" :label="opt.label" :value="opt.value" />
        </el-select>
      </el-form-item>
      <el-form-item label="状态">
        <el-switch v-model="createForm.is_active" active-text="启用" inactive-text="禁用" />
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button @click="createOpen = false">取消</el-button>
      <el-button type="primary" @click="submitCreate">创建</el-button>
    </template>
  </el-dialog>

  <el-dialog v-model="editOpen" title="编辑账号" width="520px">
    <el-form :model="editForm" label-width="90px">
      <el-form-item label="用户名">
        <el-input v-model="editForm.username" disabled />
      </el-form-item>
      <el-form-item label="角色">
        <el-select v-model="editForm.roles" multiple filterable style="width: 100%" placeholder="选择角色">
          <el-option v-for="opt in allowedRoleOptions" :key="opt.value" :label="opt.label" :value="opt.value" />
        </el-select>
      </el-form-item>
      <el-form-item label="状态">
        <el-switch v-model="editForm.is_active" active-text="启用" inactive-text="禁用" />
      </el-form-item>
      <el-form-item label="重置密码">
        <el-input v-model="editForm.password" placeholder="留空则不修改" show-password />
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button @click="editOpen = false">取消</el-button>
      <el-button type="primary" @click="submitEdit">保存</el-button>
    </template>
  </el-dialog>

  <el-dialog v-model="roleEditOpen" title="编辑角色权限" width="640px">
    <el-form :model="roleEditForm" label-width="90px">
      <el-form-item label="角色">
        <el-input v-model="roleEditForm.name" disabled />
      </el-form-item>
      <el-form-item label="权限码">
        <el-select v-model="roleEditForm.permission_codes" multiple filterable style="width: 100%" placeholder="选择或搜索权限码">
          <el-option v-for="opt in permissionOptions" :key="opt.value" :label="opt.label" :value="opt.value" />
        </el-select>
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button @click="roleEditOpen = false">取消</el-button>
      <el-button type="primary" @click="submitRolePerms">保存</el-button>
    </template>
  </el-dialog>
</template>
